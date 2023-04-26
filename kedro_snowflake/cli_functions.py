import json
import os
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
    mgr: KedroContextManager
    with KedroContextManager(ctx.metadata.package_name, ctx.env) as mgr:
        generator = SnowflakePipelineGenerator(
            pipeline,
            ctx.env,
            mgr.plugin_config,
            resolve_connection_params_from_config(mgr),
            mgr.context.params,
            extra_params,
            extra_env,
        )
        yield mgr, generator.generate()


def resolve_connection_params_from_config(mgr):
    """Uses either credentials.yml or environment variables to resolve connection parameters
    (especially password for Snowflake)"""
    if c := mgr.plugin_config.snowflake.connection.credentials:
        connection_params = mgr.context.config_loader["credentials"].get(c, None)
        if not connection_params:
            raise ValueError(f"Credentials {c} not found in credentials.yml")
    else:
        password = os.environ.get(
            (pass_env := mgr.plugin_config.snowflake.connection.password_from_env),
            None,
        )
        if not password:
            raise ValueError(
                f"Environment variable for password is not set or empty: {pass_env}"
            )

        connection_params = mgr.plugin_config.snowflake.connection.copy(
            exclude={"credentials", "password_from_env"},
            update={"password": password},
        ).dict(by_alias=True)
    return connection_params
