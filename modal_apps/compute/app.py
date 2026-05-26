"""Modal app invoked by Dagster.

Invoked by the Dagster `ModalComputeComponent` via
``modal run modal_apps/compute/app.py::process_assets``. The function runs
on Modal's infrastructure; it uses ``dagster-pipes`` to stream logs and
asset materializations back to the Dagster run that launched it.

Key Modal mechanics:

- The function is decorated with ``@app.function``, which means Modal runs
  it remotely (in a cloud container) when invoked via ``modal run``.
- The Dagster Pipes session is communicated to the remote function through
  env vars and the Pipes message channel that
  ``dagster_modal.ModalClient`` (a ``PipesSubprocessClient``) sets up
  around the ``modal run`` subprocess.
- The container image must include ``dagster-pipes`` so the remote code
  can ``open_dagster_pipes()``.
"""

import modal
from dagster_pipes import open_dagster_pipes

app = modal.App("lambda-compute-demo-modal")

# Image must include dagster-pipes so the remote function can talk back
# to Dagster. Add any real-work dependencies (pandas, duckdb, etc.) here.
image = modal.Image.debian_slim().pip_install("dagster-pipes")


@app.function(image=image, timeout=900)
def process_assets() -> None:
    """Remote Modal function — does the real compute.

    Inputs (selected asset keys, custom extras) arrive via the Pipes
    ``extras`` channel. Outputs (logs, materializations, metadata) flow
    back through the same Pipes session.
    """
    with open_dagster_pipes() as pipes:
        extras = pipes.get_extra("selected_asset_keys") or []
        # Also accessible: pipes.get_extra("source"), etc., for whatever
        # the component passed in its `extras` dict.

        pipes.log.info(f"Modal received {len(extras)} asset(s): {extras}")

        for asset_key in extras:
            pipes.log.info(f"Computing {asset_key} on Modal")
            # ...your real work goes here (S3, DB queries, ML inference,
            # GPU jobs, whatever Modal is good at)...
            pipes.report_asset_materialization(
                asset_key=asset_key,
                metadata={
                    "row_count": 0,        # replace with the real count
                    "modal_app": app.name,
                },
            )
