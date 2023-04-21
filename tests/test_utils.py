import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
from kedro.config import OmegaConfigLoader
from kedro.framework.context import KedroContext

from kedro_snowflake.config import KedroSnowflakeConfig
from kedro_snowflake.utils import compress_folder_to_zip, KedroContextManager


@pytest.mark.parametrize("exclude", [None, [".pyc"]])
def test_can_zip_folder(exclude):
    with TemporaryDirectory() as source, TemporaryDirectory() as target, TemporaryDirectory() as extract:
        ((s := Path(source)) / "test.txt").write_text("test")
        (s / "test.pyc").write_text("test2")
        compress_folder_to_zip(source, Path(target) / "test.zip", exclude=exclude)
        with zipfile.ZipFile(Path(target) / "test.zip", "r") as zip_ref:
            zip_ref.extractall(extract)
            assert (Path(extract) / "test.txt").read_text() == "test"
            assert (Path(extract) / "test.pyc").exists() == (exclude is None)


def test_can_create_context_manager(patched_kedro_package):
    with KedroContextManager("tests", "base") as mgr:
        assert mgr is not None and isinstance(
            mgr, KedroContextManager
        ), "Invalid object returned"
        assert isinstance(mgr.context, KedroContext), "No KedroContext"
        assert isinstance(
            mgr.plugin_config, KedroSnowflakeConfig
        ), "Invalid plugin config"


def test_can_create_context_manager_with_omegaconf(patched_kedro_package):
    with KedroContextManager("tests", "base") as mgr:
        with patch.object(mgr, "context") as context:
            context.mock_add_spec(KedroContext)
            context.config_loader = OmegaConfigLoader(
                str(Path.cwd() / "conf"),
                config_patterns={"snowflake": ["snowflake*"]},
                default_run_env="base",
            )
            assert isinstance(mgr.context, KedroContext), "No KedroContext"
            assert isinstance(
                mgr.plugin_config, KedroSnowflakeConfig
            ), "Invalid plugin config"
