import os
from pathlib import Path
from unittest.mock import call, patch
from uuid import uuid4

import yaml
from click.testing import CliRunner
from kedro_snowflake import cli
from kedro_snowflake.config import KedroSnowflakeConfig

from tests.utils import (
    create_kedro_conf_dirs,
    has_any_calls_matching_predicate,
)


def test_can_initialize_basic_plugin_config(
    patched_kedro_package, cli_context, tmp_path: Path,
):

    config_path = create_kedro_conf_dirs(tmp_path)
    unique_id = uuid4().hex
    with patch.object(Path, "cwd", return_value=tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli.init,
            [
                f"account_{unique_id}",
                f"user_{unique_id}",
                f"password_from_env_{unique_id}",
                f"database_{unique_id}",
                f"schema_{unique_id}",
                f"warehouse_{unique_id}",
            ],
            obj=cli_context,
        )
        assert result.exit_code == 0

        snow_config_path = config_path / "snowflake.yml"
        assert (
            snow_config_path.exists() and snow_config_path.is_file()
        ), f"{snow_config_path.absolute()} is not a valid file"

        config: KedroSnowflakeConfig = KedroSnowflakeConfig.parse_obj(
            yaml.safe_load(snow_config_path.read_text())
        )
        assert config.snowflake.connection.account == f"account_{unique_id}"
        assert config.snowflake.connection.user == f"user_{unique_id}"
        assert (
            config.snowflake.connection.password_from_env
            == f"password_from_env_{unique_id}"
        )
        assert config.snowflake.connection.database == f"database_{unique_id}"
        assert config.snowflake.connection.schema_ == f"schema_{unique_id}"
        assert config.snowflake.connection.warehouse == f"warehouse_{unique_id}"
        assert all(
            s.startswith("@") and len(s) >= 2
            for s in (
                config.snowflake.runtime.stage,
                config.snowflake.runtime.temporary_stage,
            )
        )

        assert all(
            k in config.snowflake.runtime.dependencies.packages
            for k in ("snowflake-snowpark-python", "zstandard", "fsspec")
        )
        assert all(
            k in config.snowflake.runtime.dependencies.imports
            for k in ("kedro", "kedro_snowflake", "kedro_datasets")
        )


@patch("snowflake.snowpark.session.Session")
def test_can_dry_run_pipeline(
    snowpark_session, patched_kedro_package, cli_context, tmp_path: Path, dummy_pipeline
):
    runner = CliRunner()
    output_path = tmp_path / "pipeline.sql"
    with patch.dict(
        os.environ, {"SNOWFLAKE_PASSWORD": "test_password"}, clear=False
    ), patch.dict("kedro.framework.project.pipelines", {"__default__": dummy_pipeline}):
        result = runner.invoke(
            cli.run, ["--dry-run", "-o", str(output_path.absolute())], obj=cli_context,
        )
        assert result.exit_code == 0

        assert "execution skipped (--dry-run)" in result.output
        # assert that snowpark_session.sql method was NOT called with an x argument
        assert not has_any_calls_matching_predicate(
            snowpark_session.SessionBuilder().configs().create().sql,
            lambda call_: any(
                any(
                    sql.lower() in arg.lower()
                    for sql in (
                        "execute task",
                        "alter task",
                        "call SYSTEM$TASK_DEPENDENTS_ENABLE",
                    )
                )
                for arg in call_.args
                if isinstance(arg, str)
            ),
        ), "It seems like there was a call with execute/alter/call system in the dry-run"

        assert (
            output_path.exists()
            and output_path.is_file()
            and output_path.lstat().st_size > 100
        ), f"{output_path.absolute()} is not a valid file"


@patch("snowflake.snowpark.session.Session")
def test_can_run_pipeline(
    snowpark_session, patched_kedro_package, cli_context, tmp_path: Path, dummy_pipeline
):
    output_path = tmp_path / "pipeline.sql"
    runner = CliRunner()
    with patch.dict(
        os.environ, {"SNOWFLAKE_PASSWORD": "test_password"}, clear=False
    ), patch.dict("kedro.framework.project.pipelines", {"__default__": dummy_pipeline}):
        result = runner.invoke(
            cli.run, ["-o", str(output_path.absolute())], obj=cli_context,
        )
        assert result.exit_code == 0

        assert "execution skipped (--dry-run)" not in result.output
        for sql in ("execute task", "alter task", "call SYSTEM$TASK_DEPENDENTS_ENABLE"):
            assert has_any_calls_matching_predicate(
                snowpark_session.SessionBuilder().configs().create().sql,
                lambda call_: any(
                    arg.lower().startswith(sql.lower())
                    for arg in call_.args
                    if isinstance(arg, str)
                ),
            ), f"No calls with {sql} detected"
        assert (
            output_path.exists()
            and output_path.is_file()
            and output_path.lstat().st_size > 100
            and output_path.read_text()
        ), f"{output_path.absolute()} is not a valid file"
