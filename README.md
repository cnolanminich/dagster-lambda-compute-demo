# lambda_compute_demo

A Dagster project that demonstrates a **thin-orchestrator** pattern: Dagster
runs execute as **non-isolated runs** inside the existing code location
container, and the actual compute is offloaded to an **AWS Lambda** function
via Dagster Pipes.

The Dagster run does almost no work locally — it serializes the selected asset
keys into a Pipes event, invokes Lambda, and relays the materialization
results / logs that Lambda streams back.

---

## How it works

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Dagster+ deployment                                     │
│                                                         │
│  ┌────────────────────────────────────────────────┐     │
│  │ Code location container (long-running)         │     │
│  │                                                │     │
│  │   ┌──────────────────────────────────────┐     │     │
│  │   │ Non-isolated run process             │     │     │
│  │   │   1. Build Pipes event                │     │     │
│  │   │   2. invoke_lambda(function_name)     │ ────┼──┐  │
│  │   │   3. Forward logs / materializations  │ ◀───┼──┤  │
│  │   └──────────────────────────────────────┘     │  │  │
│  └────────────────────────────────────────────────┘  │  │
└──────────────────────────────────────────────────────┼──┘
                                                       │
                                       ┌───────────────▼───────────────┐
                                       │ AWS Lambda function           │
                                       │   open_dagster_pipes()        │
                                       │   …do the real work…          │
                                       │   report_asset_materialization│
                                       └───────────────────────────────┘
```

### The pieces

1. **`LambdaComputeComponent`** — `src/lambda_compute_demo/components/lambda_compute_component.py`.
   Custom component that:
   - Accepts a `function_name`, optional `region_name`, optional `event`, and
     a list of `AssetSpec`s in its YAML schema.
   - Builds a `@dg.multi_asset` (with `can_subset=True`) whose body invokes
     Lambda via `dagster_aws.pipes.PipesLambdaClient`.
   - Wraps the assets in a `define_asset_job` tagged
     `dagster/isolation: non-isolated`, so launches default to non-isolated.

2. **Instance** — `src/lambda_compute_demo/defs/lambda_jobs/defs.yaml`.
   Concrete component instance defining two assets
   (`warehouse/raw/orders`, `warehouse/raw/customers`) that share one Lambda
   invocation. The `function_name` and `region_name` are pulled from env vars
   via `{{ env.LAMBDA_FUNCTION_NAME }}` / `{{ env.AWS_REGION }}`.

3. **`deployment_settings.yaml`** — Enables non-isolated runs at the
   deployment level (`non_isolated_runs.enabled: true`) and caps concurrency.

4. **`dagster_cloud.yaml`** — Code location manifest for Dagster+.

### Dagster Pipes (the part that makes Lambda feel native)

The Lambda function is expected to use the
[`dagster-pipes`](https://docs.dagster.io/concepts/dagster-pipes) runtime:

```python
# Inside the AWS Lambda handler:
from dagster_pipes import open_dagster_pipes

def handler(event, context):
    with open_dagster_pipes() as pipes:
        pipes.log.info("Running compute in Lambda")
        # ...do the real work...
        pipes.report_asset_materialization(
            metadata={"row_count": 12_345},
            asset_key="warehouse/raw/orders",
        )
```

`PipesLambdaClient.run(...).get_results()` yields the `MaterializeResult`s
reported by the function, so Dagster sees them as if they came from a normal
asset body.

---

## Why non-isolated runs?

Dagster+ supports two run modes:

| Mode               | Where it runs                                  | Cold start             |
|--------------------|------------------------------------------------|------------------------|
| **Isolated**       | A fresh, per-run pod / container               | ~20–60 s typical       |
| **Non-isolated**   | The existing code location container          | <1 s (just a subprocess) |

For this pattern non-isolated is the right default: the run isn't running any
real compute, so paying 20–60 s to provision an isolated pod just to call
`boto3.client("lambda").invoke(...)` is pure overhead. With non-isolated runs
the launch-to-Lambda-invocation latency is essentially the cost of starting
a Python subprocess inside the code server.

The trade-offs of non-isolated runs:

- ✅ Near-instant startup
- ✅ Cheaper (no extra pod / task overhead)
- ✅ Great for orchestration-only workloads (Pipes, REST API calls, dbt Cloud
  triggers, Fivetran syncs, etc.)
- ⚠️ Shares CPU/memory with the code server — keep concurrency capped
  (`max_concurrent_non_isolated_runs` in `deployment_settings.yaml`).
- ⚠️ A crashing run can affect the code server. Fine here because the
  Python code is just an `invoke` call.

---

## Latency expectations

End-to-end latency for materializing an asset = **run startup + invocation
overhead + actual compute time + Pipes teardown**. Approximate numbers for
common patterns:

| Pattern                                    | Run startup    | Invocation overhead | Compute ceiling      | Typical end-to-end for a 30 s job |
|--------------------------------------------|----------------|---------------------|----------------------|-----------------------------------|
| **Non-isolated run → Lambda (this repo)**  | < 1 s          | 50–500 ms (warm Lambda) / 1–10 s (cold start) | 15 min (Lambda hard limit) | **~31–35 s**                      |
| Isolated run → Lambda                      | 20–60 s        | 50–500 ms / 1–10 s cold | 15 min               | ~51–95 s                          |
| Non-isolated run → ECS Fargate task        | < 1 s          | 30–90 s (task provisioning) | unlimited            | ~61–121 s                         |
| Isolated run → ECS Fargate task            | 20–60 s        | 30–90 s             | unlimited            | ~81–181 s                         |
| Isolated run → in-pod compute (no Pipes)   | 20–60 s        | n/a                 | unlimited            | ~51–91 s                          |
| Non-isolated run → in-process compute      | < 1 s          | n/a                 | bounded by code server resources | ~31 s                             |

Notes on the numbers:

- **Lambda warm vs. cold** — A warm Lambda invocation returns in tens to a
  few hundred milliseconds. A cold start (no recent invocation, or a recently
  updated function) can add 1–10 s depending on package size, language, and
  whether the function is in a VPC. Provisioned concurrency removes cold
  starts entirely.
- **Lambda 15-minute hard cap** — If your compute can run longer than 15
  minutes, Lambda isn't the right backend. Move to ECS, EKS, Batch, or
  Databricks via the corresponding Pipes client. The component pattern
  stays the same — only the client changes.
- **ECS Fargate task launch** — Provisioning a new Fargate task is on the
  order of 30–90 seconds for a normal image; longer for large images or
  cold subnets. Using an always-on ECS service with a queue can avoid this,
  but at the cost of always paying for the compute.
- **Isolated run startup** is dominated by pulling the code location image
  and starting the run worker. With small images it can be ~15 s; with
  heavy data-science images it routinely hits a minute.

### When does this pattern win?

The Lambda + non-isolated combo dominates when **per-task compute is small
and you have many of them**. Examples:

- Lots of small partitioned assets (one Lambda per partition).
- Fan-out workflows: thousands of API enrichments, file conversions,
  per-row inferences.
- Event-driven workloads where you care about end-to-end latency more than
  absolute throughput.

It loses to ECS/Batch when:

- A single task needs > 15 min, > 10 GB RAM, or a GPU.
- The Python environment / OS packages are too large to fit a Lambda
  deployment package or container image (10 GB cap).
- You need persistent local disk beyond `/tmp` (512 MB by default, up to
  10 GB configurable).

### Throughput

Lambda can scale to thousands of concurrent invocations within the account
limit. The Dagster-side bottleneck is `max_concurrent_non_isolated_runs`
(default we set: 5) — bump it if you need higher fan-out, but remember
each non-isolated run consumes a subprocess in the code server container.

---

## Getting started

### 1. Install dependencies

This project pins Python 3.10 (see `.python-version`) because one of the
transitive dependencies (`yarl==1.24.0`) only ships `cp310` wheels on macOS
ARM at the time of writing.

```bash
uv sync --group dev
```

### 2. Configure AWS credentials & env vars

The component uses the standard boto3 credential chain.

```bash
export AWS_REGION=us-east-1
export LAMBDA_FUNCTION_NAME=my-dagster-lambda
# Plus whatever your auth setup needs: AWS_PROFILE, SSO login, etc.
```

### 3. Deploy a Pipes-aware Lambda function

Your Lambda just needs the `dagster-pipes` package and an
`open_dagster_pipes()` block. See the snippet under
[Dagster Pipes](#dagster-pipes-the-part-that-makes-lambda-feel-native).

### 4. Run locally

```bash
uv run dg dev
```

Open http://localhost:3000 and materialize the assets in
`warehouse/raw/*`. (Local `dg dev` runs are always in-process — non-isolated
runs are a deployment-level concept that takes effect once you deploy to
Dagster+.)

### 5. Deploy to Dagster+

```bash
# Push code
uv run dg plus deploy

# Enable non-isolated runs at the deployment level
uv run dg plus deployment settings set-from-file deployment_settings.yaml
```

After this, any run of `lambda_my_dagster_lambda_job` (the job auto-created
by the component) will be launched as a non-isolated run because the job is
tagged `dagster/isolation: non-isolated`.

---

## Project structure

```
lambda_compute_demo/
├── dagster_cloud.yaml              # Dagster+ code location manifest
├── deployment_settings.yaml        # non_isolated_runs.enabled = true
├── pyproject.toml                  # dagster, dagster-aws, dagster-modal, boto3
├── src/lambda_compute_demo/
│   ├── components/
│   │   ├── lambda_compute_component.py           # Lambda base (CI deploys infra)
│   │   ├── lambda_with_provisioning_component.py # Lambda + manual CDK provision
│   │   ├── modal_compute_component.py            # Modal base (ephemeral `modal run`)
│   │   └── modal_with_deploy_component.py        # Modal + manual `modal deploy`
│   └── defs/
│       ├── lambda_jobs/                  # Lambda base instance
│       ├── lambda_jobs_with_provisioning/  # Lambda + provision instance
│       ├── modal_jobs/                   # Modal base instance
│       └── modal_jobs_with_deploy/       # Modal + deploy instance
├── lambdas/
│   └── compute/
│       ├── handler.py              # Pipes-aware Lambda handler
│       └── requirements.txt        # dagster-pipes
├── modal_apps/
│   └── compute/
│       ├── app.py                  # Pipes-aware Modal function
│       └── requirements.txt        # modal, dagster-pipes (for the host)
├── infra/
│   ├── cdk_app.py                  # CDK app entrypoint
│   ├── lambda_stack.py             # CDK stack defining the Lambda
│   ├── requirements.txt            # aws-cdk-lib, lambda-python-alpha
│   └── cdk.json                    # CDK config
├── .github/workflows/
│   └── deploy.yml                  # CI: prod / staging / branch / cleanup
└── .env.example                    # Local-dev env-var template
```

## Deploying the Lambda function

Dagster doesn't provision AWS infrastructure — the Lambda function has to
be created out of band. This project uses **AWS CDK (Python)** colocated
under `infra/` to manage the Lambda lifecycle, and a GitHub Actions
workflow ties everything together across environments.

### Multi-environment strategy

The CDK stack and the Dagster component are both parametrized by an
**environment name** that matches the Dagster+ deployment name:

| Environment | CDK stack | Lambda function name | Lifecycle |
|---|---|---|---|
| Prod | `LambdaComputeDemoStack-prod` | `lambda-compute-demo-compute-prod` | Deployed on push to `main` |
| Staging | `LambdaComputeDemoStack-staging` | `lambda-compute-demo-compute-staging` | Deployed on push to `staging` |
| Branch deploy (per PR) | `LambdaComputeDemoStack-pr-N` | `lambda-compute-demo-compute-pr-N` | Deployed on PR open, destroyed on PR close |
| Local dev | n/a (point at shared staging) | `lambda-compute-demo-compute-staging` | n/a |

### Single source of truth: deployment name

`DAGSTER_CLOUD_DEPLOYMENT_NAME` is set automatically by Dagster+ at runtime
in every code location container. The component derives the Lambda name
from it:

```yaml
# defs.yaml
function_name: "{{ env('LAMBDA_FUNCTION_NAME', 'lambda-compute-demo-compute-' + env('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local')) }}"
```

Meanwhile CDK names its function the same way via `-c env=$DEPLOYMENT`.
As long as the CI workflow uses matching env names on both sides, the two
connect automatically — **no env-var injection or CDK output piping
required**.

`LAMBDA_FUNCTION_NAME` can still be set explicitly to override the
convention (useful for local dev pointing at a shared staging Lambda).

### Deploying manually

```bash
# Prod
cd infra && cdk deploy -c env=prod
cd .. && uv run dg plus deploy

# Per-PR branch
cd infra && cdk deploy -c env=pr-123
cd .. && uv run dg plus deploy   # auto-detects PR context

# Personal dev stack
cd infra && cdk deploy -c env=$USER
```

### Deploying via CI

`.github/workflows/deploy.yml` handles four event types:

| GitHub event | CDK action | Dagster+ action |
|---|---|---|
| Push to `main` | `cdk deploy -c env=prod` | `dg plus deploy` → prod deployment |
| Push to `staging` | `cdk deploy -c env=staging` | `dg plus deploy` → staging deployment |
| PR opened/synced | `cdk deploy -c env=pr-N` | `dg plus deploy` → branch deployment |
| PR closed | `cdk destroy -c env=pr-N --force` | (Dagster+ auto-tears down branch deployment) |

Lambda is always deployed first (`needs: [classify, deploy-lambda]`) so
the function exists when the code location boots.

Required GitHub secrets:

- `AWS_ROLE_ARN` — IAM role assumed via OIDC (CloudFormation, Lambda, IAM,
  Logs permissions).
- `AWS_REGION` — e.g. `us-east-1`.
- `DAGSTER_CLOUD_API_TOKEN` — `dg plus create ci-api-token`.
- `DAGSTER_CLOUD_ORGANIZATION`, `DAGSTER_CLOUD_DEPLOYMENT`.

### Local development

Three options, in order of how you'd typically use them:

1. **Point at shared staging Lambda** (fastest for Dagster-side iteration):
   ```bash
   cp .env.example .env
   # .env: LAMBDA_FUNCTION_NAME=lambda-compute-demo-compute-staging
   uv run dg dev
   ```
2. **Personal CDK stack** (when iterating on Lambda code):
   ```bash
   cd infra && cdk deploy -c env=$USER
   # .env: LAMBDA_FUNCTION_NAME=lambda-compute-demo-compute-cdykinski
   ```
3. **LocalStack / SAM local** — possible but the component would need a
   custom boto3 client with `endpoint_url` overridden. Only worth it for
   heavy Lambda iteration without AWS round-trips.

Local `dg dev` runs are always in-process (the non-isolated/isolated
distinction only applies to Dagster+ deployments).

### Costs to watch

- **Stale branch Lambdas.** A PR sitting open for weeks = a Lambda +
  CloudWatch log group sitting around. Lambda itself is free when idle,
  but VPC ENIs and provisioned concurrency do cost. The `pull_request:
  [closed]` handler in the workflow runs `cdk destroy` to clean up.
- **CloudWatch log retention** for `pr-*` stacks is set to 1 day vs. 1
  week for prod — tweak in `infra/lambda_stack.py` if you need different.

## Two patterns: CI-managed vs. Dagster-managed infrastructure

This project ships two component variants for the same end behavior. Pick
based on the operational story you want:

| | `LambdaComputeComponent` | `LambdaWithProvisioningComponent` |
|---|---|---|
| Who deploys the Lambda | CI (CDK runs in GitHub Actions) | A Dagster asset (CDK runs inside a Dagster run) |
| When the Lambda updates | On every push to main / staging / PR | When an operator clicks "Materialize" on the infra asset |
| Recommended for | Production | Demos, sandboxes, single-button ops workflows |
| Risks of broken deploys | Stops CI pipeline | Fails a Dagster run; asset history reflects infra state |
| Image requirements | Code location image stays slim | Code location image must include Node.js + AWS CDK CLI |

### `LambdaWithProvisioningComponent` — Dagster provisions its own infra

Defined in `src/lambda_compute_demo/components/lambda_with_provisioning_component.py`,
this variant subclasses the base component and adds **one extra asset**:

```
infrastructure/LambdaComputeDemoStack-<env>     ← new, manual-only
   └─── warehouse/raw/orders_provisioned         ← deps on infra asset
   └─── warehouse/raw/customers_provisioned      ← deps on infra asset
```

When you click **Materialize** on the infra asset:

1. The asset body shells out to `cdk deploy -c env=<env>
   --require-approval never --outputs-file /tmp/cdk-outputs-<env>.json`
   inside `infra/`.
2. Stdout streams into the Dagster run logs.
3. CloudFormation outputs (function name, ARN) are parsed and attached as
   `MaterializeResult` metadata, so you can see what got deployed.

### How "manual-only" is enforced

The infra asset is intentionally never auto-materialized:

- **No `AutomationCondition`** — declarative automation skips it.
- **Excluded from the compute job** — schedules / sensors that target the
  compute assets do not pull in the infra asset.
- **Tagged `manual_only=true`** — visible in the UI as a marker.
- **No upstream → downstream auto-materialization** — Dagster only
  materializes upstream assets when a downstream's automation condition
  asks for it. With no condition anywhere in the chain, the infra asset
  only ever materializes when you click the button.

The compute assets still list the infra asset as a `dep` so the lineage
shows the relationship — but materializing them does **not** trigger a
`cdk deploy`. If you materialize the compute assets before ever clicking
the infra "Materialize" button, the Pipes invocation will fail with
`ResourceNotFoundException`, which is the expected and informative
failure mode.

### Runtime requirements for the variant

For the Dagster-managed-infra variant to actually work, the code location
container needs:

- **Node.js 20+** and `npm install -g aws-cdk@2`.
- **Python 3.10+** and `pip install -r infra/requirements.txt` (already
  installed via the project's own deps if you bundle them).
- **AWS credentials** mounted into the container (Dagster+ env vars or an
  IAM role attached to the code location task).
- **IAM permissions** for CDK: CloudFormation full access, Lambda create/
  update, IAM `PassRole` for the Lambda execution role, plus S3 access
  to the CDK staging bucket.

If the `cdk` CLI isn't on `PATH`, the asset fails fast with a clear
`Failure` message rather than producing a confusing traceback.

### When to use which

- **Stick with `LambdaComputeComponent` + CI/CD for prod.** Standard
  separation of concerns. CI rollback = git revert. Infra failures don't
  show up in data lineage.
- **Use `LambdaWithProvisioningComponent` for demos, sandboxes, or
  internal tools.** A reviewer can click two buttons (provision → run)
  to see the whole loop end-to-end without leaving the Dagster UI. Also
  useful when the team running the pipeline owns the infra and doesn't
  want a GitHub Actions hop in the middle.
- **Don't mix them in the same Dagster+ deployment** unless you mean to —
  both variants will try to manage the same CloudFormation stack and
  you'll get fights between CI and the manual asset. The project ships
  with both component instances active only for documentation purposes
  — delete one of `src/lambda_compute_demo/defs/lambda_jobs/` or
  `src/lambda_compute_demo/defs/lambda_jobs_with_provisioning/` in real
  use.

## Modal variant

This project also ships **two Modal components** that mirror the Lambda
ones one-for-one, using the `dagster-modal` integration. Same Pipes
pattern, same non-isolated-run tagging — different serverless backend.

### Components

| Lambda variant | Modal variant | What it does |
|---|---|---|
| `LambdaComputeComponent` | `ModalComputeComponent` | Non-isolated Dagster run → Pipes → invoke serverless function |
| `LambdaWithProvisioningComponent` | `ModalWithDeployComponent` | + manual-only "deploy" asset |

The Modal components live in `src/lambda_compute_demo/components/modal_*.py`
and the Modal app code lives in `modal_apps/compute/app.py`.

### How invocation works (Modal vs. Lambda)

The integration is a thin wrapper around the Modal CLI:
[`dagster_modal.ModalClient`](https://docs.dagster.io/api/python-api/libraries/dagster-modal#dagster_modal.ModalClient)
extends `PipesSubprocessClient` and runs `modal run <func_ref>` as a
subprocess of the Dagster run. Pipes context flows through env vars set
on that subprocess, materializations and logs flow back through the Pipes
message channel.

```python
# Inside the Modal app (modal_apps/compute/app.py):
@app.function(image=image, timeout=900)
def process_assets() -> None:
    with open_dagster_pipes() as pipes:
        for asset_key in pipes.get_extra("selected_asset_keys") or []:
            # ...real work on Modal's infra...
            pipes.report_asset_materialization(asset_key=asset_key, metadata={...})
```

### Key behavioral differences from Lambda

| | Lambda + Pipes | Modal + Pipes |
|---|---|---|
| Pre-deploy required? | **Yes** — Lambda must exist; you call it by ARN/name | **No** — `modal run` ships local code on each invocation |
| Invocation mechanism | `boto3.client("lambda").invoke(...)` (HTTP API) | `subprocess.Popen(["modal", "run", ...])` (CLI) |
| Hard timeout | **15 min** | **24 hours** (default; configurable) |
| Max memory | **10 GB** | **300+ GB** (depending on plan) |
| GPU support | No | **Yes** (A10/A100/H100) |
| Container image | OCI image, 10 GB cap | OCI image, 50+ GB cap |
| Cold start | 100 ms – 10 s | 1 – 30 s (longer for big images) |
| Concurrency model | One invocation = one container | One invocation = N containers (Modal `.map()`) |
| Cost model | Per-invocation + per-GB-second | Per-GPU/CPU-second, billed by Modal |

### Latency comparison

Adding Modal rows to the latency table from earlier:

| Pattern | Run startup | Invocation overhead | Compute ceiling | Typical end-to-end for a 30 s job |
|---|---|---|---|---|
| Non-isolated run → Lambda (warm) | < 1 s | 50–500 ms | 15 min | ~31–35 s |
| Non-isolated run → Lambda (cold) | < 1 s | 1–10 s | 15 min | ~32–41 s |
| **Non-isolated run → Modal (warm)** | **< 1 s** | **0.5–2 s** (CLI overhead) | **24 h** | **~32–34 s** |
| **Non-isolated run → Modal (cold)** | **< 1 s** | **5–30 s** | **24 h** | **~36–60 s** |
| Non-isolated run → ECS Fargate task | < 1 s | 30–90 s | unlimited | ~61–121 s |

Notes:

- Modal's **CLI overhead** (~0.5–2 s warm) is higher than Lambda's HTTP
  invoke (~50–500 ms warm) because the `modal run` subprocess does code
  hash checking, image lookup, and bidirectional log streaming setup.
- Modal **cold starts vary wildly** with image size — a slim Python image
  cold-starts in 1–3 s, a multi-GB image with CUDA + models takes 20–30 s.
  Use `keep_warm=N` on `@app.function` to hold N containers warm.
- For **fast, small, stateless work**, Lambda is still the latency king.
  For **long, GPU-heavy, large-memory** workloads, Modal wins by being
  the only option that allows it at all.

### When to use Modal over Lambda

- You need **GPUs** (Modal has them; Lambda doesn't).
- You need **> 15 min compute** (Lambda's hard cap is 15 min).
- You need **> 10 GB memory** or **large container images** (Modal allows
  much bigger).
- You want to develop in Python end-to-end without CDK / IaC ceremony —
  Modal's "deploy = `modal deploy app.py`" model is dramatically simpler
  than Lambda + CloudFormation.
- You want **Modal's `.map()` fan-out**: a single function call spawning
  N parallel containers, all reporting back through one Pipes session.

### When to stay with Lambda

- You're already deep in AWS and want IAM/VPC integration without a
  third-party hop.
- You need **very low latency per invocation** (Lambda HTTP invoke beats
  Modal CLI invoke).
- Your compute is small and frequent — Lambda's pricing is cheaper at
  high volume for tiny invocations.

### The `ModalWithDeployComponent` variant

Same manual-only enforcement as the Lambda variant — the deploy asset:

- Has no `AutomationCondition` (declarative automation skips it).
- Is excluded from the compute job (schedules skip it).
- Is tagged `manual_only=true`.
- Is listed as a `dep` of compute assets purely for lineage.

When materialized, it shells out to:

```
modal deploy --env <modal_env> modal_apps/compute/app.py
```

Stdout streams into the Dagster run logs and the result lands as
`MaterializeResult` metadata (app file, env, func_ref, deploy log tail).

### Multi-environment story for Modal

Modal has its own concept of **environments** (per-workspace namespaces).
The `modal_env` field is wired up the same way the Lambda function name
is — derived from `DAGSTER_CLOUD_DEPLOYMENT_NAME` with an explicit
override env var (`MODAL_ENVIRONMENT`):

```yaml
modal_env: "{{ env('MODAL_ENVIRONMENT', env('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'main')) }}"
```

So the same convention used for Lambda + CDK applies: Dagster+ `prod`
deployment talks to Modal env `prod`, `pr-123` talks to Modal env
`pr-123`, etc. Modal envs are cheaper to create than per-PR
CloudFormation stacks (no CFN provisioning overhead), so per-PR Modal
environments are even more practical than per-PR Lambdas.

### Runtime requirements

The Dagster code location container needs:

- **`modal` CLI on PATH** — `pip install modal` puts it there.
- **Modal auth** — `MODAL_TOKEN_ID` + `MODAL_TOKEN_SECRET` env vars
  (set in Dagster+ deployment settings), or `~/.modal.toml` for local dev
  (`modal token new`).

No Node.js or CDK CLI needed for the base Modal pattern. Only the
`ModalWithDeployComponent` variant needs `modal` available at runtime
(same as the Lambda variant needs `cdk` available at runtime).

## Learn more

- [Dagster Pipes overview](https://docs.dagster.io/concepts/dagster-pipes)
- [PipesLambdaClient API](https://docs.dagster.io/api/python-api/libraries/dagster-aws#dagster_aws.pipes.PipesLambdaClient)
- [dagster-modal `ModalClient`](https://docs.dagster.io/api/python-api/libraries/dagster-modal)
- [Modal documentation](https://modal.com/docs)
- [Non-isolated runs (Dagster+)](https://docs.dagster.io/dagster-plus/deployment/management/non-isolated-runs)
- [Dagster Components](https://docs.dagster.io/concepts/components)
