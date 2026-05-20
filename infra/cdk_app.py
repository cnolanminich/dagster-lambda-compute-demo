"""CDK app entrypoint.

The stack is parametrized by an environment name (``prod``, ``staging``,
``pr-123``, ``$USER`` for personal dev stacks, etc.) so that multiple
environments can coexist in the same AWS account without colliding.

Usage:

    cdk deploy -c env=prod
    cdk deploy -c env=pr-123
    cdk destroy -c env=pr-123

If ``-c env=...`` is omitted, the ``CDK_ENV`` environment variable is used,
falling back to ``local``.
"""

import os

import aws_cdk as cdk

from lambda_stack import LambdaComputeStack

app = cdk.App()

env_name = app.node.try_get_context("env") or os.environ.get("CDK_ENV") or "local"

LambdaComputeStack(
    app,
    f"LambdaComputeDemoStack-{env_name}",
    env_name=env_name,
)

app.synth()
