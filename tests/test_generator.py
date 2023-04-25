import json
from unittest.mock import patch
from uuid import UUID

from kedro_snowflake.generator import SnowflakePipelineGenerator
from kedro_snowflake.pipeline import KedroSnowflakePipeline
from snowflake.snowpark import Session

from tests.utils import get_arg_type


def test_can_generate_pipeline(
    patched_snowflake_pipeline_generator: SnowflakePipelineGenerator,
):
    g = patched_snowflake_pipeline_generator
    ks_pipeline: KedroSnowflakePipeline = (
        patched_snowflake_pipeline_generator.generate()
    )
    assert isinstance(ks_pipeline, KedroSnowflakePipeline)
    assert len(ks_pipeline.pipeline_task_names) == len(g.get_kedro_pipeline().nodes)
    assert all(
        isinstance(sql, str)
        for sql in ks_pipeline.pipeline_tasks_sql + ks_pipeline.execute_sql
    )


def test_generate_calls_sproc(
    patched_snowflake_pipeline_generator: SnowflakePipelineGenerator,
):
    g = patched_snowflake_pipeline_generator
    g.generate()
    assert (
        g.snowflake_session.sproc.register.call_count
        == 2  # 1x for KEDRO_START (generating run id), 1x for KEDRO_RUN (generic)
    ), "Stored procedure number of calls doesn't match"


def test_kedro_run_sproc_is_valid(
    patched_snowflake_pipeline_generator: SnowflakePipelineGenerator,
):
    g = patched_snowflake_pipeline_generator
    g._construct_kedro_snowflake_sproc(
        imports=["i1", "i2"],
        packages=["p1", "p2", "p3"],
        stage_location="@TEST_STAGE",
        temp_data_stage="@TEST_TEMP_STAGE",
    )
    with patch("kedro.framework.session"), patch(
        "kedro.framework.startup.bootstrap_project"
    ), patch("os.chdir"):
        # check if kedro_run_sproc.func is callable
        fn = g.snowflake_session.method_calls[0].args[0]
        assert get_arg_type(fn, 0) == Session
        assert callable(fn)
        result = fn(g.snowflake_session, "env", "run-id-123", "default", ["node1"], "")
        assert isinstance(result, str) and isinstance(
            json.loads(result), dict
        ), "Cannot deserialize result of kedro_run_sproc"
        # check if first argument of `fn` is of type Session


def test_kedro_start_run_sproc_is_valid(
    patched_snowflake_pipeline_generator: SnowflakePipelineGenerator,
):
    g = patched_snowflake_pipeline_generator
    g._construct_kedro_snowflake_root_sproc(stage_location="@TEST_STAGE")
    fn = g.snowflake_session.method_calls[0].args[0]
    assert get_arg_type(fn, 0) == Session
    assert callable(fn)
    result = fn(g.snowflake_session)
    g.snowflake_session.sql.assert_called_once()
    assert isinstance(result, str) and isinstance(
        UUID(result), UUID
    ), "Result is not a valid UUID"  # UUID will throw, when invalid
