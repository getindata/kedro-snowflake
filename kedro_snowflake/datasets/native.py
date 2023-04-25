import logging
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional, Union

import snowflake.snowpark as sp
from kedro.io import AbstractDataSet
from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    DataSetError,
    parse_dataset_definition,
)
from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


class SnowflakeStageFileDataSet(AbstractDataSet):
    # init with params: snowflake_stage, path, dataset, filepath_arg
    def __init__(
        self,
        stage: str,
        filepath: str,
        dataset: Union[str, dict],
        filepath_arg: str = "filepath",
        database: Optional[str] = None,
        schema: Optional[str] = None,
        credentials: Dict[str, Any] = None,
    ):
        assert stage.startswith("@"), "snowflake_stage must start with '@'"
        self._snowflake_stage = stage
        self._path = filepath
        self._dataset = dataset
        self._filepath_arg = filepath_arg

        connection_parameters = credentials
        if credentials is None and not (database and schema):
            raise DataSetError(
                f"'{self.__class__.__name__}' requires either 'credentials' or "
                f"'database' and 'schema' to be specified."
            )
        connection_parameters.update({"database": database, "schema": schema})
        self._connection_parameters = connection_parameters

        ds = dataset
        if isinstance(dataset, str):
            ds = {"type": dataset}
        elif isinstance(dataset, DictConfig):  # handling OmegaConfigLoader
            ds = OmegaConf.to_container(dataset)
        assert isinstance(
            ds, dict
        ), "There's an issue with the config loader - could not parse `dataset` param as dictionary"
        self._dataset_type, self._dataset_config = parse_dataset_definition(ds)
        if VERSION_KEY in self._dataset_config:
            raise DataSetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

    def _describe(self) -> Dict[str, Any]:
        return {
            "dataset_type": self._dataset_type.__name__,
            "dataset_config": self._dataset_config,
        }

    @property
    def _target_path(self) -> str:
        return f"{self._snowflake_stage}/{self._path}"

    @property
    def _snowflake_session(self) -> sp.Session:
        try:
            logger.debug("Trying to reuse active snowpark session...")
            session = sp.context.get_active_session()
        except sp.exceptions.SnowparkSessionException:
            logger.debug("No active snowpark session found. Creating")
            session = sp.Session.builder.configs(self._connection_parameters).create()
        return session

    def _construct_dataset(self, target_path: Path):
        ds_config = deepcopy(self._dataset_config)
        ds_config[self._filepath_arg] = str(target_path.absolute())
        dataset = self._dataset_type(**ds_config)
        return dataset

    @contextmanager
    def _wrapped_dataset(self):
        with TemporaryDirectory() as tmpdir:
            tmp_file_path = Path(tmpdir) / Path(self._path).name
            dataset = self._construct_dataset(tmp_file_path)
            yield dataset, tmp_file_path

    def _load(self) -> Any:
        with self._wrapped_dataset() as (dataset, tmp_file_path):
            self._snowflake_session.file.get(
                self._target_path, str(tmp_file_path.parent.absolute())
            )
            return dataset.load()

    def _save(self, data: Any) -> None:
        with self._wrapped_dataset() as (dataset, tmp_file_path):
            dataset.save(data)
            self._snowflake_session.file.put(
                str(tmp_file_path.absolute()),
                self._target_path,
                auto_compress=False,
                overwrite=True,
            )
