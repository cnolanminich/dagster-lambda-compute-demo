"""Custom component that materializes assets by invoking AWS Lambda.

The Dagster run itself is intended to execute as a `non-isolated` run inside the
code location container — it does no real work locally. Instead, it forwards
each asset materialization to an AWS Lambda function via Dagster Pipes, which
streams logs and metadata back into Dagster while the Lambda performs the
actual compute.
"""

from collections.abc import Iterable, Mapping
from typing import Any

import boto3
import dagster as dg
from dagster_aws.pipes import PipesLambdaClient

# Run tag understood by Dagster+ to request a non-isolated run.
NON_ISOLATED_RUN_TAGS: Mapping[str, str] = {"dagster/isolation": "non-isolated"}


class LambdaComputeComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materializes one or more assets by invoking an AWS Lambda function.

    Each instance of this component produces a multi-asset whose execution
    triggers a single AWS Lambda invocation through Dagster Pipes. The Lambda
    function is expected to use the ``dagster-pipes`` runtime (or any Pipes
    client implementation) so that logs and asset metadata flow back into the
    Dagster run.

    Attributes:
        function_name: Name (or ARN) of the AWS Lambda function to invoke.
        specs: AssetSpecs produced by this component.
        region_name: AWS region the Lambda lives in. Optional — defaults to the
            standard boto3 resolution chain (env vars, ``~/.aws/config``, etc.).
        event: Optional JSON-serializable payload merged into the Pipes event
            sent to Lambda. Useful for passing per-asset configuration.
    """

    function_name: str
    specs: list[dg.ResolvedAssetSpec]
    region_name: str | None = None
    event: dict[str, Any] | None = None

    def _lambda_client(self) -> PipesLambdaClient:
        """Construct a Pipes-wrapped Lambda client for invocation."""
        boto_client = boto3.client("lambda", region_name=self.region_name)
        return PipesLambdaClient(client=boto_client)

    def execute(
        self,
        context: dg.AssetExecutionContext,
        pipes_client: PipesLambdaClient,
    ) -> Iterable[dg.MaterializeResult]:
        """Trigger the Lambda invocation and yield Pipes results back to Dagster."""
        payload = {
            "selected_asset_keys": [
                key.to_user_string() for key in context.selected_asset_keys
            ],
            **(self.event or {}),
        }
        context.log.info(
            f"Invoking AWS Lambda '{self.function_name}' for "
            f"{len(context.selected_asset_keys)} asset(s)."
        )
        yield from pipes_client.run(
            function_name=self.function_name,
            event=payload,
            context=context,
        ).get_results()

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        specs = self.specs
        function_name = self.function_name

        @dg.multi_asset(
            name=f"lambda_{function_name.replace('-', '_').replace(':', '_')}",
            specs=specs,
            can_subset=True,
        )
        def _assets(context: dg.AssetExecutionContext):
            yield from self.execute(
                context=context,
                pipes_client=self._lambda_client(),
            )

        # Define an explicit job that tags every launched run as non-isolated.
        # Materializing these assets via this job (or via the Dagster+ launchpad
        # with non-isolated runs enabled) keeps the run inside the existing
        # code location container — all the heavy lifting happens in Lambda.
        job = dg.define_asset_job(
            name=f"{_assets.op.name}_job",
            selection=[spec.key for spec in specs],
            tags=dict(NON_ISOLATED_RUN_TAGS),
        )

        return dg.Definitions(assets=[_assets], jobs=[job])
