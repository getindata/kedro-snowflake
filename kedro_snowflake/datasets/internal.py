import logging
from functools import cached_property
from io import BytesIO
from sys import version_info
from typing import Any, Dict, Union

import backoff
import cloudpickle
import zstandard as zstd
from kedro.io import AbstractDataSet
from snowflake.snowpark import DataFrame as SnowParkDataFrame
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

logger = logging.getLogger()


class SnowflakeTransientTableDataSet(AbstractDataSet):
    def __init__(
        self,
        dataset_name: str,
        snowflake_stage: str,
        run_id: str,
        run_id_column_name: str,
        snowflake_session: Session,
    ):
        self.run_id_column_name = run_id_column_name
        self.dataset_name = dataset_name
        self.snowflake_session: Session = snowflake_session
        self.snowflake_stage = snowflake_stage
        self.run_id = run_id

    @cached_property
    def table_name(self):
        return f"kedro_tmp_{self.dataset_name}"

    def table_exists(self):
        result = self.snowflake_session._conn.run_query(
            "select 1 from information_schema.tables where UPPER(table_name) = UPPER(?)",
            params=(self.table_name,),
        )

        return len(result["data"]) >= 1

    def _load(self) -> Union[SnowParkDataFrame, Any]:
        return self.snowflake_session.table(self.table_name).drop(
            self.run_id_column_name
        )

    def _save(self, data: Union[SnowParkDataFrame, Any]) -> None:
        df: SnowParkDataFrame = data.withColumn(
            self.run_id_column_name, F.lit(self.run_id)
        )
        df.write.save_as_table(
            self.table_name, mode="overwrite", table_type="transient"
        )

    def _describe(self) -> Dict[str, Any]:
        return {"table_name": self.table_name}


class SnowflakeStagePickleDataSet(AbstractDataSet):
    def __init__(
        self,
        dataset_name: str,
        snowflake_stage: str,
        run_id: str,
        snowflake_session: Session,
    ):
        self.dataset_name = dataset_name
        self.snowflake_session: Session = snowflake_session
        self.snowflake_stage = snowflake_stage
        self.run_id = run_id
        self.pickle_protocol = None if version_info[:2] > (3, 8) else 4

    @cached_property
    def target_stage_location(self):
        return f"{self.snowflake_stage}/kedro-snowflake-storage/{self.run_id}"

    @cached_property
    def target_name(self):
        return f"{self.dataset_name}.pkl"

    @cached_property
    def target_path(self):
        return f"{self.target_stage_location}/{self.target_name}"

    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def _load(self):
        with zstd.open(
            self.snowflake_session.file.get_stream(self.target_path),
            "rb",
        ) as stream:
            return cloudpickle.load(stream)

    def _save(self, data: Any) -> None:
        with BytesIO() as buffer:
            with zstd.open(
                buffer, "wb", cctx=zstd.ZstdCompressor(level=5), closefd=False
            ) as stream:
                cloudpickle.dump(data, stream, protocol=self.pickle_protocol)
            buffer.flush()
            buffer.seek(0)
            setattr(buffer, "name", self.target_name)
            self.snowflake_session.file.put_stream(
                buffer,
                self.target_path,
                auto_compress=False,
                overwrite=True,
            )

    def _describe(self) -> Dict[str, Any]:
        return {
            "info": "for use only within Snowflake",
            "dataset_name": self.dataset_name,
            "path": self.target_stage_location,
        }


class SnowflakeRunnerDataSet(AbstractDataSet):
    def __init__(
        self,
        dataset_name: str,
        snowflake_stage: str,
        run_id: str,
        snowflake_session: Session,
        run_id_column_name: str,
    ):
        self.run_id_column_name = run_id_column_name
        self.dataset_name = dataset_name
        self.snowflake_session: Session = snowflake_session
        self.snowflake_stage = snowflake_stage
        self.run_id = run_id

    def _transient_ds(self) -> SnowflakeTransientTableDataSet:
        return SnowflakeTransientTableDataSet(
            dataset_name=self.dataset_name,
            snowflake_stage=self.snowflake_stage,
            run_id=self.run_id,
            run_id_column_name=self.run_id_column_name,
            snowflake_session=self.snowflake_session,
        )

    def _pickle_ds(self) -> SnowflakeStagePickleDataSet:
        return SnowflakeStagePickleDataSet(
            dataset_name=self.dataset_name,
            snowflake_stage=self.snowflake_stage,
            run_id=self.run_id,
            snowflake_session=self.snowflake_session,
        )

    def _load(self):
        if (tds := self._transient_ds()).table_exists():
            return tds.load()
        else:
            return self._pickle_ds().load()

    def _save(self, data) -> None:
        if isinstance(data, SnowParkDataFrame):
            ds = self._transient_ds()
            logger.info(f"Saving into transient table {ds.table_name} [{self.run_id}]")
        else:
            ds = self._pickle_ds()
            logger.info(f"Saving into stage {ds.target_path} [{self.run_id}]")
        ds.save(data)

    def _describe(self) -> Dict[str, Any]:
        return {
            "info": "for use only within Snowflake",
            "dataset_name": self.dataset_name,
        }
