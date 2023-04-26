import json
import os
from pathlib import Path
from typing import Tuple

import click

from kedro_snowflake.cli_functions import (
    context_and_pipeline,
    parse_extra_env_params,
    parse_extra_params,
)
from kedro_snowflake.config import CONFIG_TEMPLATE_YAML
from kedro_snowflake.misc import CliContext
from kedro_snowflake.pipeline import KedroSnowflakePipeline


@click.group("Snowflake")
def commands():
    """Kedro plugin adding support for Snowflake / Snowpark"""
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
def snowflake_group(ctx, metadata, env):
    ctx.obj = CliContext(env, metadata)


@snowflake_group.command()
@click.argument("account")
@click.argument("user")
@click.argument("password_from_env")
@click.argument("database")
@click.argument("schema")
@click.argument("warehouse")
@click.pass_obj
def init(
    ctx: CliContext,
    account: str,
    user: str,
    password_from_env: str,
    database: str,
    schema: str,
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
    "--dry-run",
    "dry_run",
    is_flag=True,
    help="Only save SQL definition, do not run it (use --output to specify file)",
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
@click.option(
    "--wait-for-completion",
    is_flag=True,
    help="Block the terminal until the pipeline is completed",
)
@click.option(
    "--timeout",
    type=int,
    help="Timeout in seconds for the pipeline to complete (used only with --wait-for-completion)",
    default=600,
)
@click.pass_obj
def run(
    ctx: CliContext,
    pipeline: str,
    params: str,
    dry_run: bool,
    output: str,
    env_var: Tuple[str],
    wait_for_completion: bool,
    timeout: int,
):
    """Runs the pipeline using Snowflake Tasks"""
    params = json.dumps(p) if (p := parse_extra_params(params)) else ""
    extra_env = parse_extra_env_params(env_var)

    click.echo(
        f"Converting Kedro pipeline {pipeline} into Snowflake tasks...{os.linesep}"
        "This may take a while if warehouse is stopped, please be patient..."
    )

    snowflake_pipeline: KedroSnowflakePipeline
    with context_and_pipeline(ctx, pipeline, extra_env, params) as (
        mgr,
        snowflake_pipeline,
    ):
        exit_code = 0
        if not dry_run:
            success = snowflake_pipeline.run(
                wait_for_completion,
                timeout,
                echo_fn=lambda s: (click.clear(), click.echo(s)),
                on_start_callback=lambda: click.echo(
                    "Snowflake tasks execution started"
                ),
            )
            exit_code = 0 if success else 1
        else:
            click.echo(
                click.style(
                    "Snowflake tasks execution skipped (--dry-run)", fg="yellow"
                )
            )
        try:
            snowflake_pipeline.save(Path(output))
            click.echo(f"Snowflake tasks generated into {output}")
        except Exception as e:
            click.echo(click.style(f"Could not save tasks SQL into {output}", fg="red"))
            raise e
        exit(exit_code)
