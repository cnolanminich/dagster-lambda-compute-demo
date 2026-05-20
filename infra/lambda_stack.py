"""CDK stack defining the AWS Lambda function used by the Dagster project.

The function name is ``lambda-compute-demo-compute-{env_name}`` so the same
stack template can deploy independent copies for prod, staging, per-PR
branch deployments, and personal dev environments without collisions.

The naming convention is shared with the Dagster component instance —
``src/lambda_compute_demo/defs/lambda_jobs/defs.yaml`` derives the same
name from ``DAGSTER_CLOUD_DEPLOYMENT_NAME`` at runtime, so no env-var
injection is needed in CI: as long as the CDK ``env`` matches the Dagster+
deployment name, things connect automatically.
"""

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct

FUNCTION_NAME_PREFIX = "lambda-compute-demo-compute"
LAMBDAS_DIR = Path(__file__).resolve().parent.parent / "lambdas" / "compute"


def function_name_for_env(env_name: str) -> str:
    """Single source of truth for the per-env Lambda function name."""
    return f"{FUNCTION_NAME_PREFIX}-{env_name}"


class LambdaComputeStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        function_name = function_name_for_env(env_name)

        # Branch-deploy Lambdas are ephemeral and should be cleanable; prod
        # stacks should retain logs longer. Tweak as you like.
        log_retention = (
            logs.RetentionDays.ONE_DAY
            if env_name.startswith("pr-")
            else logs.RetentionDays.ONE_WEEK
        )

        fn = PythonFunction(
            self,
            "ComputeFn",
            function_name=function_name,
            entry=str(LAMBDAS_DIR),
            index="handler.py",
            handler="handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            memory_size=1024,
            timeout=cdk.Duration.minutes(15),
            log_retention=log_retention,
            description=(
                f"Compute backend invoked by Dagster ({env_name}) "
                "via PipesLambdaClient."
            ),
        )

        fn.role.add_managed_policy(  # type: ignore[union-attr]
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        cdk.Tags.of(fn).add("dagster:env", env_name)
        cdk.Tags.of(fn).add("dagster:project", "lambda-compute-demo")

        cdk.CfnOutput(self, "FunctionName", value=fn.function_name)
        cdk.CfnOutput(self, "FunctionArn", value=fn.function_arn)
        cdk.CfnOutput(self, "EnvName", value=env_name)
