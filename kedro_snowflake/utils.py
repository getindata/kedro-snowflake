import importlib
import shutil
import tarfile
from functools import cached_property
from pathlib import Path
from typing import Optional, List
from uuid import uuid4
import zipfile
import os
import zstandard as zstd
from kedro.config import AbstractConfigLoader, ConfigLoader, OmegaConfigLoader
from kedro.framework.session import KedroSession
from omegaconf import DictConfig, OmegaConf

from kedro_snowflake.config import (
    KedroSnowflakeConfig,
    KEDRO_SNOWFLAKE_CONFIG_PATTERN,
    KEDRO_SNOWFLAKE_CONFIG_KEY,
)


def compress_folder_to_zip(path, zip_path, exclude=None):
    exclude = exclude or []
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zip_file:
        for root, dirs, files in os.walk(path):
            for file in files:
                if not any(file.endswith(pattern) for pattern in exclude):
                    file_path = os.path.join(root, file)
                    zip_file.write(file_path, arcname=os.path.relpath(file_path, path))
        # add the top-level folder to the archive
        zip_file.write(path, arcname=os.path.basename(path))


def zstd_folder(
    folder_to_compress: Path,
    output_dir: Path,
    file_name: Optional[str] = None,
    level=5,
    exclude=None,
) -> Path:
    """Compress a folder using zstandard and return the path to the archive."""
    tar_path = output_dir / (file_name or (uuid4().hex + ".tar.zst"))
    with zstd.open(tar_path, "wb", cctx=zstd.ZstdCompressor(level=level)) as archive:
        with tarfile.open(fileobj=archive, mode="w") as tar:

            def filter_fn(tarinfo):
                if any(
                    [tarinfo.name.endswith(excluded) for excluded in (exclude or [])]
                ):
                    return None
                else:
                    return tarinfo

            tar.add(
                folder_to_compress, arcname=folder_to_compress.name, filter=filter_fn
            )
    return tar_path


def zip_dependencies(dependencies: List[str], output_dir: Path):
    assert output_dir.is_dir(), f"{output_dir} is not a directory"

    results = {dependency: get_module_path(dependency) for dependency in dependencies}

    # compress the dependencies as zip
    for dependency, path in results.items():
        if path.is_dir():
            compress_folder_to_zip(
                path, output_dir / f"{dependency}.zip", [".pyc", "__pycache__"]
            )
        else:
            shutil.copyfile(path, output_dir / path.name)


def get_module_path(module_name) -> Path:
    module = importlib.import_module(module_name)
    try:
        path = module.__path__[0]
    except AttributeError:
        path = module.__file__
    return Path(path)


class KedroContextManager:
    def __init__(
        self, package_name: str, env: str, extra_params: Optional[dict] = None
    ):
        self.extra_params = extra_params
        self.env = env
        self.package_name = package_name
        self.session: Optional[KedroSession] = None

    @cached_property
    def context(self):
        assert self.session is not None, "Session not initialized yet"
        return self.session.load_context()

    @cached_property
    def plugin_config(self) -> KedroSnowflakeConfig:
        cl: AbstractConfigLoader = self.context.config_loader
        try:
            obj = self.context.config_loader.get(KEDRO_SNOWFLAKE_CONFIG_PATTERN)
        except:
            obj = None

        if obj is None:
            try:
                obj = self.context.config_loader[KEDRO_SNOWFLAKE_CONFIG_KEY]
                if isinstance(cl, OmegaConfigLoader):
                    obj = OmegaConf.to_container(obj)
            except KeyError:
                obj = None

        if obj is None:
            if not isinstance(cl, ConfigLoader):
                raise ValueError(
                    f"You're using a custom config loader: {cl.__class__.__qualname__}{os.linesep}"
                    f"you need to add the snowflake config to it.{os.linesep}"
                    f"Make sure you add snowflake* to config_pattern in CONFIG_LOADER_ARGS in the settings.py file.{os.linesep}"
                    """Example:
CONFIG_LOADER_ARGS = {
    # other args
    "config_patterns": {"snowflake": ["snowflake*"]}
}
                    """.strip()
                )
            else:
                raise ValueError(
                    "Missing snowflake.yml files in configuration. Make sure that you configure your project first"
                )
        return KedroSnowflakeConfig.parse_obj(obj)

    def __enter__(self):
        self.session = KedroSession.create(
            self.package_name, env=self.env, extra_params=self.extra_params
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)
