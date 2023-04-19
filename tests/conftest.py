import os
from collections import defaultdict
from unittest.mock import patch

from kedro.pipeline import Pipeline, pipeline, node
from pytest import fixture

from kedro_snowflake.config import (
    KedroSnowflakeConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
    SnowflakeRuntimeConfig,
    DependenciesConfig,
)
from kedro_snowflake.generator import SnowflakePipelineGenerator
from tests.utils import identity


@fixture()
def dummy_pipeline() -> Pipeline:
    return pipeline(
        [
            node(identity, inputs="input_data", outputs="i2", name="node1"),
            node(identity, inputs="i2", outputs="i3", name="node2"),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )


@fixture()
def mock_plugin_config():
    return KedroSnowflakeConfig(
        snowflake=SnowflakeConfig(
            connection=SnowflakeConnectionConfig(
                account="test_account",
                user="test_user",
                password_from_env="SNOWFLAKE_PASSWORD",
                database="test_database",
                warehouse="test_warehouse",
                schema="test_schema",
            ),
            runtime=SnowflakeRuntimeConfig(dependencies=DependenciesConfig()),
        )
    )


@fixture()
def patched_kedro_package():
    with patch("kedro.framework.project.PACKAGE_NAME", "tests") as patched_package:
        original_dir = os.getcwd()
        os.chdir("tests")
        yield patched_package
        os.chdir(original_dir)


@fixture()
def patched_snowflake_pipeline_generator(
    mock_plugin_config: KedroSnowflakeConfig, dummy_pipeline
):
    with patch("snowflake.snowpark.session.Session") as session, patch.dict(
        os.environ, {"SNOWFLAKE_PASSWORD": "test_password"}
    ), patch.dict(
        "kedro.framework.project.pipelines",
        {"__default__": dummy_pipeline, "test_pipeline": dummy_pipeline},
    ):
        generator = SnowflakePipelineGenerator(
            "test_pipeline",
            "test_env",
            mock_plugin_config,
            mock_plugin_config.snowflake.connection.copy(
                exclude={"credentials", "password_from_env"},
                update={"password": os.environ.get("SNOWFLAKE_PASSWORD")},
            ).dict(by_alias=True),
            {},
            None,
            None,
        )
        yield generator
