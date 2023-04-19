from unittest.mock import patch, MagicMock, Mock
from uuid import uuid4

import pandas as pd
import pytest
from kedro.io import DataSetError
from omegaconf import DictConfig

from kedro_snowflake.datasets.internal import SnowflakeRunnerDataSet
from kedro_snowflake.datasets.native import SnowflakeStageFileDataSet
from snowflake.snowpark import DataFrame as SnowParkDataFrame


@pytest.mark.parametrize(
    "dataset_to_wrap,data_example",
    (
        ({"type": "pandas.CSVDataSet"}, (dummy_df := pd.DataFrame({"a": [1, 2, 3]}))),
        ({"type": "text.TextDataSet"}, "bla bla bla :)"),
        ("pandas.CSVDataSet", dummy_df),
        ({"type": "pandas.CSVDataSet", "save_args": {"index": False}}, dummy_df),
        ({"type": "pickle.PickleDataSet"}, {"a": 1, "b": 2, "c": 3}),
        (DictConfig({"type": "pandas.CSVDataSet"}), dummy_df),
    ),
)
@patch("snowflake.snowpark")
def test_can_use_snowflake_stage_wrapped_dataset(
    sp, dataset_to_wrap, data_example, tmpdir
):
    TemporaryDirectoryMock = MagicMock()
    TemporaryDirectoryMock.__enter__.return_value = tmpdir

    with patch(
        "kedro_snowflake.datasets.native.TemporaryDirectory", TemporaryDirectoryMock
    ):
        ds: SnowflakeStageFileDataSet = SnowflakeStageFileDataSet(
            "@TEST_STAGE", "my/file.txt", dataset_to_wrap, credentials=MagicMock()
        )
        ds.save(data_example)
        assert ds._snowflake_session.file.put.call_count == 1

        data = ds.load()
        assert ds._snowflake_session.file.get.call_count == 1
        if isinstance(data_example, pd.DataFrame):
            pd.testing.assert_frame_equal(data, data_example)
        else:
            assert data == data_example, "Objects are not equal after loading"


@pytest.mark.parametrize(
    "invalid_constructor",
    [
        lambda: SnowflakeStageFileDataSet(
            "@TEST_STAGE",
            "my/file.txt",
            {"type": "pandas.CSVDataSet", "versioned": True},
            credentials=MagicMock(),
        ),
        lambda: SnowflakeStageFileDataSet(
            "TEST_STAGE",
            "my/file.txt",
            {"type": "pandas.CSVDataSet"},
            credentials=MagicMock(),
        ),
        lambda: SnowflakeStageFileDataSet(
            "@TEST_STAGE",
            "my/file.txt",
            None,
            credentials=MagicMock(),
        ),
        lambda: SnowflakeStageFileDataSet(
            "@TEST_STAGE",
            "my/file.txt",
            {"asd": 123},
            credentials=MagicMock(),
        ),
        lambda: SnowflakeStageFileDataSet(
            "@TEST_STAGE", "my/file.txt", "pandas.CSVDataSet"
        ),
    ],
)
def test_wrapper_dataset_invalid_configs(invalid_constructor):
    with pytest.raises((ValueError, DataSetError, AssertionError)):
        invalid_constructor()


@pytest.mark.parametrize(
    "data_to_save",
    [
        pd.DataFrame({"a": [1, 2, 3]}),
        "some dummy text",
        object(),
        {"a": 1, "b": 2, "c": 3},
        lambda session: session.create_dataframe(pd.DataFrame({"a": [1, 2, 3]})),
    ],
)
def test_runner_dataset_auto_switch_based_on_data_type(data_to_save):
    with patch("snowflake.snowpark.Session") as session:
        session.create_dataframe.return_value = Mock(spec=SnowParkDataFrame)
        ds = SnowflakeRunnerDataSet(
            "test_ds", "@TEST_STAGE", uuid4().hex, session, "run_id_column"
        )
        if callable(data_to_save):
            data = data_to_save(session)
        else:
            data = data_to_save

        ds.save(data)
        if isinstance(data, SnowParkDataFrame):
            data.withColumn().write.save_as_table.assert_called_once()
        else:
            session.file.put_stream.assert_called_once()
