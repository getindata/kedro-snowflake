from typing import Any, Dict

from kedro.io import AbstractDataSet, DataCatalog
from kedro.pipeline import Pipeline
from kedro.runner import SequentialRunner
from pluggy import PluginManager
from snowflake.snowpark import Session

from kedro_snowflake.datasets.internal import SnowflakeRunnerDataSet


class SnowflakeRunner(SequentialRunner):
    def __init__(
        self,
        snowflake_session: Session,
        snowflake_stage: str,
        run_id: str,
        run_id_column_name: str = "kedro_snowflake_run_id",
        is_async: bool = False,
    ):
        super().__init__(is_async)
        self.run_id_column_name = run_id_column_name
        self.run_id = run_id
        self.snowflake_stage = snowflake_stage
        self.snowflake_session = snowflake_session

    def create_default_data_set(self, ds_name: str) -> AbstractDataSet:
        return SnowflakeRunnerDataSet(
            ds_name,
            self.snowflake_stage,
            self.run_id,
            self.snowflake_session,
            self.run_id_column_name,
        )

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        session_id: str = None,
    ) -> Dict[str, Any]:
        unsatisfied = pipeline.inputs() - set(catalog.list())
        for ds_name in unsatisfied:
            catalog = catalog.shallow_copy()
            catalog.add(ds_name, self.create_default_data_set(ds_name))

        return super().run(pipeline, catalog, hook_manager, session_id)
