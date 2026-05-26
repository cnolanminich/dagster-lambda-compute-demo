"""Alternate Modal component that ALSO deploys the Modal app via `modal deploy`.

The Modal analog of `LambdaWithProvisioningComponent`. Materializing the
deploy asset runs `modal deploy <app_file>`, pushing the app to Modal's
infrastructure as a persistent named app. The compute assets list it as
a dep purely for lineage — they do not auto-trigger a deploy.

When is this variant useful?

- You want a **persistent named Modal app** (e.g. so other systems can
  invoke it, or for stable cold-start behavior).
- You want a single "Materialize" button in the Dagster UI to push code
  changes to Modal without leaving the orchestration tool.
- You want Modal-side scheduled functions, web endpoints, or `keep_warm`
  containers — all of which require `modal deploy` first.

When is the base `ModalComputeComponent` enough?

- You're happy with ephemeral `modal run` invocations where Modal ships
  the local code each time. No deploy step needed.

Runtime requirements (code location container must have):

- ``modal`` CLI on PATH (``pip install modal`` is enough — the package
  ships the CLI).
- Valid Modal auth: ``MODAL_TOKEN_ID`` + ``MODAL_TOKEN_SECRET`` env vars
  (or ``~/.modal.toml`` for local dev).
"""

import os
import shutil
import subprocess
from pathlib import Path

import dagster as dg

from lambda_compute_demo.components.modal_compute_component import (
    NON_ISOLATED_RUN_TAGS,
    ModalComputeComponent,
    _safe_op_suffix,
)


class ModalWithDeployComponent(ModalComputeComponent):
    """ModalComputeComponent + a manual-only `modal deploy` asset."""

    # Path to the Modal app file to deploy, relative to ``project_directory``.
    # e.g. ``modal_apps/compute/app.py``.
    modal_app: str

    # Used for the deploy asset key (``infrastructure/modal_<name>_<env>``)
    # and for log/metadata clarity. Defaults to the app filename stem.
    modal_app_name: str | None = None

    # Optional override for the deploy asset key. If omitted, derived from
    # ``modal_app_name`` and ``modal_env``.
    deploy_asset_key: dg.ResolvedAssetKey | None = None

    # ─────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────

    def _resolved_app_name(self) -> str:
        return self.modal_app_name or Path(self.modal_app).stem

    def _resolved_deploy_key(self) -> dg.AssetKey:
        if self.deploy_asset_key is not None:
            return self.deploy_asset_key
        return dg.AssetKey(
            [
                "infrastructure",
                f"modal_{self._resolved_app_name()}_{self.modal_env or 'default'}",
            ]
        )

    # ─────────────────────────────────────────────────────────────────
    # The deploy asset
    # ─────────────────────────────────────────────────────────────────

    def _build_deploy_asset(self) -> dg.AssetsDefinition:
        deploy_key = self._resolved_deploy_key()
        modal_app = self.modal_app
        modal_env = self.modal_env
        func_ref = self.func_ref
        project_dir = self._resolved_project_dir()
        app_name = self._resolved_app_name()

        @dg.asset(
            key=deploy_key,
            description=(
                f"Deploys the Modal app at `{modal_app}` via "
                f"`modal deploy` (env={modal_env or 'default'}). "
                "**Manual-only** — never auto-materialized. Re-materialize "
                "to push code or image changes to Modal."
            ),
            group_name="infrastructure",
            kinds={"modal", "deployment"},
            tags={"manual_only": "true"},
            # No automation_condition → declarative automation skips this.
            # Not in the compute job below → schedules skip it too.
        )
        def _deploy(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if not shutil.which("modal"):
                raise dg.Failure(
                    description=(
                        "`modal` CLI not on PATH. The code location image "
                        "needs `pip install modal` (or equivalent) for this "
                        "asset to materialize."
                    ),
                )

            app_path = project_dir / modal_app
            if not app_path.exists():
                raise dg.Failure(
                    description=(
                        f"Modal app file not found at {app_path}. Check "
                        "`project_directory` and `modal_app` in defs.yaml."
                    ),
                )

            cmd = ["modal", "deploy"]
            if modal_env:
                cmd.extend(["--env", modal_env])
            cmd.append(modal_app)

            context.log.info(f"Running: {' '.join(cmd)} (cwd={project_dir})")

            proc = subprocess.Popen(
                cmd,
                cwd=str(project_dir),
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
                        f"modal deploy failed (exit {returncode}). Last 20 "
                        "lines of output:\n" + "\n".join(stdout_lines[-20:])
                    ),
                )

            return dg.MaterializeResult(
                metadata={
                    "modal_app_file": dg.MetadataValue.text(modal_app),
                    "modal_app_name": dg.MetadataValue.text(app_name),
                    "modal_env": dg.MetadataValue.text(modal_env or "(default)"),
                    "func_ref": dg.MetadataValue.text(func_ref),
                    "deploy_log_tail": dg.MetadataValue.md(
                        "```\n" + "\n".join(stdout_lines[-40:]) + "\n```"
                    ),
                },
            )

        return _deploy

    # ─────────────────────────────────────────────────────────────────
    # Overridden build_defs
    # ─────────────────────────────────────────────────────────────────

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        deploy_key = self._resolved_deploy_key()
        deploy_asset = self._build_deploy_asset()

        # Wire compute specs to depend on the deploy asset for lineage.
        # Materializing the compute assets will NOT auto-trigger a deploy
        # (no automation condition anywhere on the deploy asset).
        wired_specs = [
            spec.replace_attributes(deps=[*spec.deps, dg.AssetDep(deploy_key)])
            for spec in self.specs
        ]

        # Distinct op name so this variant doesn't collide with a sibling
        # `ModalComputeComponent` pointing at the same func_ref.
        op_name = f"modal_{_safe_op_suffix(self.func_ref)}_with_deploy"

        @dg.multi_asset(name=op_name, specs=wired_specs, can_subset=True)
        def _compute_assets(context: dg.AssetExecutionContext):
            yield from self.execute(
                context=context,
                modal_client=self._modal_client(),
            )

        # Job excludes the deploy asset so schedules don't trigger deploys.
        compute_job = dg.define_asset_job(
            name=f"{_compute_assets.op.name}_job",
            selection=[spec.key for spec in self.specs],
            tags=dict(NON_ISOLATED_RUN_TAGS),
        )

        return dg.Definitions(
            assets=[deploy_asset, _compute_assets],
            jobs=[compute_job],
        )
