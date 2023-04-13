import json
import os
from pathlib import Path
from typing import Tuple

import click
from kedro.framework.startup import ProjectMetadata

from kedro_snowflake.cli_functions import (
    parse_extra_params,
    parse_extra_env_params,
    context_and_pipeline,
)
from kedro_snowflake.config import CONFIG_TEMPLATE_YAML
from kedro_snowflake.misc import CliContext


@click.group("Snowflake")
def commands():
    """Kedro plugin adding support for Azure ML Pipelines"""
    pass


@commands.group(
    name="snowflake", context_settings=dict(help_option_names=["-h", "--help"])
)
@click.option(
    "-e",
    "--env",
    "env",
    type=str,
    default=lambda: os.environ.get("KEDRO_ENV", "local"),
    help="Environment to use.",
)
@click.pass_obj
@click.pass_context
def snowflake_group(ctx, metadata: ProjectMetadata, env):
    ctx.obj = CliContext(env, metadata)


@snowflake_group.command()
@click.argument("account")
@click.argument("database")
@click.argument("password_from_env")
@click.argument("schema")
@click.argument("user")
@click.argument("warehouse")
@click.pass_obj
def init(
    ctx: CliContext,
    account: str,
    database: str,
    password_from_env: str,
    schema: str,
    user: str,
    warehouse: str,
):
    """
    Creates basic configuration for Kedro Snowflake plugin
    """
    target_path = Path.cwd().joinpath("conf/base/snowflake.yml")
    cfg = CONFIG_TEMPLATE_YAML.format(
        **{
            "account": account,
            "database": database,
            "password_from_env": password_from_env,
            "schema": schema,
            "user": user,
            "warehouse": warehouse,
        }
    )
    target_path.write_text(cfg)
    click.echo(
        click.style(
            f"Created basic configuration in {target_path}{os.linesep}"
            "Follow the included comments to customize it",
            fg="green",
        )
    )


@snowflake_group.command()
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    help="Name of pipeline to run",
    default="__default__",
)
@click.option(
    "--params",
    "params",
    type=str,
    help="Parameters override in form of JSON string",
)
@click.option(
    "-o",
    "--output",
    type=click.types.Path(exists=False, dir_okay=False),
    default="pipeline.sql",
    help="Pipeline SQL definition file.",
)
@click.option(
    "--env-var",
    type=str,
    multiple=True,
    help="Environment variables to be injected in the steps, format: KEY=VALUE",
)
@click.pass_obj
def compile(
    ctx: CliContext, pipeline: str, params: str, output: str, env_var: Tuple[str]
):
    """Creates Snowflake tasks SQL (! it also creates stored procedures in Snowflake!)"""
    params = json.dumps(p) if (p := parse_extra_params(params)) else ""
    extra_env = parse_extra_env_params(env_var)
    with context_and_pipeline(ctx, pipeline, extra_env, params) as (
        mgr,
        snowflake_pipeline,
    ):
        snowflake_pipeline.save(Path(output))
        click.echo(f"Snowflake tasks generated into {output}")
