"""Alternate Lambda component that ALSO provisions the Lambda via CDK.

This is the "Dagster manages its own infrastructure" variant of
``LambdaComputeComponent``. It exposes one additional asset — the CDK stack
— whose materialization shells out to ``cdk deploy``. Each compute asset
gets an explicit dependency on the provisioning asset so the lineage shows
"the Lambda was deployed by this CDK run".

When to use this vs. the base ``LambdaComputeComponent``:

- **Base component**: CDK is deployed out-of-band by CI (preferred for
  production — clean separation of concerns, no Dagster-side infra
  blast-radius).
- **This component**: Useful for demos, sandbox environments, or workflows
  where ops folks want a single "Materialize" button to provision the
  infra. The compute assets still run the same way.

The CDK provisioning asset is intentionally **manual-only**:

- No ``AutomationCondition`` — declarative automation will never schedule it.
- It is *not* added to the generated compute job, so schedules / sensors
  on the data assets won't trigger it.
- Tagged ``manual_only=true`` for visual clarity in the UI.
- Compute assets list it as a dep so the lineage is visible, but
  materializing the compute assets does NOT auto-materialize it (Dagster
  only auto-materializes upstreams when an automation condition asks it to).

Runtime requirements (the code location image must have):

- ``cdk`` CLI on PATH (``npm install -g aws-cdk`` in the Dockerfile).
- ``python`` + the ``infra/`` CDK deps installed.
- AWS credentials available to boto3 / the CDK CLI.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import dagster as dg

from lambda_compute_demo.components.lambda_compute_component import (
    NON_ISOLATED_RUN_TAGS,
    LambdaComputeComponent,
)


class LambdaWithProvisioningComponent(LambdaComputeComponent):
    """LambdaComputeComponent + a manual-only CDK provisioning asset."""

    # Path to the CDK app, relative to the Dagster project root.
    cdk_dir: str = "infra"

    # The CDK context value passed via ``-c env=<env_name>``. Should match
    # the Dagster+ deployment name (``prod``, ``staging``, ``pr-N``, etc.)
    # so the deployed Lambda matches the one the compute assets resolve.
    env_name: str = "local"

    # The CloudFormation stack name CDK will deploy. Defaults to the
    # convention used in ``infra/cdk_app.py``.
    cdk_stack: str | None = None

    # Where the provisioning asset lives in the asset graph. Override per
    # instance if you want a different key.
    infra_asset_key: dg.ResolvedAssetKey | None = None

    # ─────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────

    def _resolved_stack(self) -> str:
        return self.cdk_stack or f"LambdaComputeDemoStack-{self.env_name}"

    def _resolved_infra_key(self) -> dg.AssetKey:
        if self.infra_asset_key is not None:
            return self.infra_asset_key
        # Default: infrastructure/<stack_name>
        return dg.AssetKey(["infrastructure", self._resolved_stack()])

    # ─────────────────────────────────────────────────────────────────
    # The provisioning asset
    # ─────────────────────────────────────────────────────────────────

    def _build_provision_asset(self) -> dg.AssetsDefinition:
        infra_key = self._resolved_infra_key()
        env_name = self.env_name
        cdk_dir = self.cdk_dir
        cdk_stack = self._resolved_stack()
        function_name = self.function_name

        @dg.asset(
            key=infra_key,
            description=(
                "Provisions / updates the AWS Lambda function "
                f"`{function_name}` via `cdk deploy -c env={env_name}`. "
                "**Manual-only** — never auto-materialized. Re-materialize "
                "to apply infra changes (memory, timeout, IAM, Lambda code)."
            ),
            group_name="infrastructure",
            kinds={"cdk", "aws"},
            tags={"manual_only": "true"},
            # No automation_condition → declarative automation skips this.
            # Not included in the compute job below → schedules skip it too.
        )
        def _provision(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            for tool in ("cdk", "python"):
                if not shutil.which(tool):
                    raise dg.Failure(
                        description=(
                            f"`{tool}` CLI not found on PATH. The code "
                            "location image must include Node.js + AWS CDK "
                            "for this asset to materialize."
                        ),
                    )

            # The Dagster code server's CWD is the project root in standard
            # Dagster+ deployments, but be defensive locally too.
            infra_path = Path(cdk_dir)
            if not infra_path.is_absolute():
                infra_path = Path.cwd() / cdk_dir
            if not infra_path.exists():
                raise dg.Failure(
                    description=f"CDK directory not found at {infra_path}",
                )

            outputs_path = Path(f"/tmp/cdk-outputs-{env_name}.json")
            cmd = [
                "cdk",
                "deploy",
                "-c",
                f"env={env_name}",
                "--require-approval",
                "never",
                "--outputs-file",
                str(outputs_path),
                cdk_stack,
            ]
            context.log.info(f"Running: {' '.join(cmd)} (cwd={infra_path})")

            # Stream output line-by-line into the Dagster logs.
            proc = subprocess.Popen(
                cmd,
                cwd=str(infra_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env={**os.environ},
            )
            stdout_lines: list[str] = []
            assert proc.stdout is not None
            for line in proc.stdout:
                stdout_lines.append(line.rstrip())
                context.log.info(line.rstrip())
            returncode = proc.wait()

            if returncode != 0:
                raise dg.Failure(
                    description=(
                        f"cdk deploy failed (exit {returncode}). "
                        "Last 20 lines of output:\n"
                        + "\n".join(stdout_lines[-20:])
                    ),
                )

            # Parse outputs file if CDK wrote one.
            stack_outputs: dict[str, str] = {}
            if outputs_path.exists():
                try:
                    parsed = json.loads(outputs_path.read_text())
                    stack_outputs = parsed.get(cdk_stack, {})
                except json.JSONDecodeError:
                    context.log.warning(
                        f"Could not parse CDK outputs at {outputs_path}"
                    )

            return dg.MaterializeResult(
                metadata={
                    "function_name": dg.MetadataValue.text(
                        stack_outputs.get("FunctionName", function_name),
                    ),
                    "function_arn": dg.MetadataValue.text(
                        stack_outputs.get("FunctionArn", "(unknown)"),
                    ),
                    "cdk_env": dg.MetadataValue.text(env_name),
                    "cdk_stack": dg.MetadataValue.text(cdk_stack),
                    "deploy_log_tail": dg.MetadataValue.md(
                        "```\n" + "\n".join(stdout_lines[-40:]) + "\n```"
                    ),
                },
            )

        return _provision

    # ─────────────────────────────────────────────────────────────────
    # Overridden build_defs
    # ─────────────────────────────────────────────────────────────────

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        infra_key = self._resolved_infra_key()
        provision_asset = self._build_provision_asset()
        function_name = self.function_name

        # Wire every compute spec to depend on the provisioning asset so the
        # lineage shows "this Lambda was deployed here". Materializing the
        # compute assets will NOT auto-materialize the infra (no automation
        # condition on the infra asset) — the dep is purely informational.
        wired_specs = [
            spec.replace_attributes(deps=[*spec.deps, dg.AssetDep(infra_key)])
            for spec in self.specs
        ]

        # Suffix the op name so this multi-asset doesn't collide with one
        # produced by a sibling `LambdaComputeComponent` instance pointing
        # at the same function.
        op_name = f"lambda_{function_name.replace('-', '_').replace(':', '_')}_with_provisioning"

        @dg.multi_asset(
            name=op_name,
            specs=wired_specs,
            can_subset=True,
        )
        def _compute_assets(context: dg.AssetExecutionContext):
            yield from self.execute(
                context=context,
                pipes_client=self._lambda_client(),
            )

        # The job only includes the data assets — the provisioning asset is
        # deliberately excluded so schedules / sensors on the data don't
        # trigger CDK deploys.
        compute_job = dg.define_asset_job(
            name=f"{_compute_assets.op.name}_job",
            selection=[spec.key for spec in self.specs],
            tags=dict(NON_ISOLATED_RUN_TAGS),
        )

        return dg.Definitions(
            assets=[provision_asset, _compute_assets],
            jobs=[compute_job],
        )
