"""Modal analog of `LambdaComputeComponent`.

Materializes assets by invoking a Modal function via Dagster Pipes. The
Dagster run executes as a `non-isolated` run inside the code location
container; the actual compute runs on Modal's infrastructure. Logs and
asset materializations flow back to Dagster through the Pipes session.

Key differences from the Lambda variant:

- **No pre-deployment required.** `modal run <func_ref>` ships the local
  code to Modal on each invocation. The base component assumes the Modal
  CLI + auth tokens are available in the code location container; no
  separate "function ARN" needs to be known up front.
- **`ModalClient` is a `PipesSubprocessClient`.** Invocation is `modal run
  <func_ref>` as a subprocess of the Dagster run, with Pipes context and
  messages flowing through that subprocess.
- **Modal "environments"** (a Modal concept similar to Dagster+
  deployments) are selected via the `MODAL_ENVIRONMENT` env var or the
  `--env` CLI flag.
"""

import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_modal import ModalClient

# Same run tag the Lambda components use so Dagster+ runs go non-isolated.
NON_ISOLATED_RUN_TAGS: Mapping[str, str] = {"dagster/isolation": "non-isolated"}


def _safe_op_suffix(s: str) -> str:
    """Turn a Modal func_ref into a valid Dagster op name suffix."""
    return (
        s.replace("::", "_")
        .replace("/", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


class ModalComputeComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materializes assets by invoking a Modal function via Dagster Pipes.

    Attributes:
        func_ref: Modal function reference. Accepts either Python module
            form (``modal_apps.compute.app::process_assets``) or file form
            (``modal_apps/compute/app.py::process_assets``). The form has
            to match what the Modal CLI accepts.
        specs: AssetSpecs produced by this component.
        project_directory: Working directory passed to ``ModalClient`` —
            where `modal run` will be executed from. Relative paths are
            resolved against the Dagster project root.
        modal_env: Optional Modal environment name (e.g. ``main``,
            ``staging``). Passed via the ``MODAL_ENVIRONMENT`` env var so
            the same code points at the right Modal workspace per Dagster+
            deployment.
        extras: Optional JSON-serializable extras forwarded to the Modal
            function through the Pipes ``extras`` channel.
    """

    func_ref: str
    specs: list[dg.ResolvedAssetSpec]
    project_directory: str = "."
    modal_env: str | None = None
    extras: dict[str, Any] | None = None

    # ─────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────

    def _resolved_project_dir(self) -> Path:
        project_dir = Path(self.project_directory)
        if not project_dir.is_absolute():
            project_dir = Path.cwd() / project_dir
        return project_dir

    def _modal_client(self) -> ModalClient:
        return ModalClient(project_directory=self._resolved_project_dir())

    def _env_for_invocation(self) -> dict[str, str]:
        env = dict(os.environ)
        if self.modal_env:
            env["MODAL_ENVIRONMENT"] = self.modal_env
        return env

    # ─────────────────────────────────────────────────────────────────
    # Execution
    # ─────────────────────────────────────────────────────────────────

    def execute(
        self,
        context: dg.AssetExecutionContext,
        modal_client: ModalClient,
    ) -> Iterable[dg.MaterializeResult]:
        """Invoke the Modal function and yield Pipes results back to Dagster."""
        extras: dict[str, Any] = {
            "selected_asset_keys": [
                k.to_user_string() for k in context.selected_asset_keys
            ],
            **(self.extras or {}),
        }
        context.log.info(
            f"Invoking Modal function `{self.func_ref}` for "
            f"{len(context.selected_asset_keys)} asset(s) "
            f"(modal_env={self.modal_env or 'default'})."
        )
        yield from modal_client.run(
            func_ref=self.func_ref,
            context=context,
            extras=extras,
            env=self._env_for_invocation(),
        ).get_results()

    # ─────────────────────────────────────────────────────────────────
    # Definitions
    # ─────────────────────────────────────────────────────────────────

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        specs = self.specs
        op_name = f"modal_{_safe_op_suffix(self.func_ref)}"

        @dg.multi_asset(name=op_name, specs=specs, can_subset=True)
        def _assets(context: dg.AssetExecutionContext):
            yield from self.execute(
                context=context,
                modal_client=self._modal_client(),
            )

        job = dg.define_asset_job(
            name=f"{_assets.op.name}_job",
            selection=[spec.key for spec in specs],
            tags=dict(NON_ISOLATED_RUN_TAGS),
        )

        return dg.Definitions(assets=[_assets], jobs=[job])
