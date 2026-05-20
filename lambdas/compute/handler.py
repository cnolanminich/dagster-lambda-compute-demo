"""AWS Lambda handler that performs the actual compute for the Dagster assets.

The Dagster non-isolated run invokes this Lambda via ``PipesLambdaClient``.
``open_dagster_pipes()`` parses the Pipes session info from the invocation
event and gives us a context object that can stream logs and asset
materializations back to Dagster through the Lambda response payload.
"""

from __future__ import annotations

from typing import Any

from dagster_pipes import (
    PipesLambdaContextLoader,
    PipesMappingParamsLoader,
    open_dagster_pipes,
)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entrypoint invoked by Dagster.

    The ``event`` payload contains the Pipes session info injected by
    ``PipesLambdaClient`` plus whatever extra fields the component added
    (``selected_asset_keys``, custom ``event`` dict, etc.).
    """
    # Pipes for Lambda uses the invocation event for the session info and
    # the response payload to send messages back to Dagster.
    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(event),
        context_loader=PipesLambdaContextLoader(),
    ) as pipes:
        selected = event.get("selected_asset_keys", [])
        pipes.log.info(f"Lambda received {len(selected)} asset(s): {selected}")

        # Do the actual compute here. For the demo we just emit a
        # materialization per selected asset.
        for asset_key in selected:
            pipes.log.info(f"Computing {asset_key} inside Lambda")
            # ...your real work goes here (S3 reads/writes, queries, etc.)...
            pipes.report_asset_materialization(
                asset_key=asset_key,
                metadata={
                    "lambda_request_id": context.aws_request_id,
                    "row_count": 0,  # replace with the real count
                },
            )

    return {"status": "ok"}
