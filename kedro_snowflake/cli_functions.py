import json
import re
from contextlib import contextmanager

import click

from kedro_snowflake.generator import SnowflakePipelineGenerator
from kedro_snowflake.utils import KedroContextManager


def parse_extra_params(params, silent=False):
    if params and (parameters := json.loads(params.strip("'"))):
        if not silent:
            click.echo(
                f"Running with extra parameters:\n{json.dumps(parameters, indent=4)}"
            )
    else:
        parameters = None
    return parameters


def parse_extra_env_params(extra_env):
    for entry in extra_env:
        if not re.match("[A-Za-z0-9_]+=.*", entry):
            raise Exception(f"Invalid env-var: {entry}, expected format: KEY=VALUE")

    return {(e := entry.split("="))[0]: e[1] for entry in extra_env}


@contextmanager
def context_and_pipeline(ctx, pipeline, extra_env, extra_params):
    with KedroContextManager(ctx.metadata.package_name, ctx.env) as mgr:
        generator = SnowflakePipelineGenerator(
            pipeline,
            ctx.env,
            mgr.plugin_config,
            mgr.context.params,
            extra_params,
            extra_env,
        )
        yield mgr, generator.generate()
