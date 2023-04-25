import inspect
from pathlib import Path
from shutil import copy
from typing import Any, Callable
from unittest.mock import MagicMock

import yaml


def identity(x):
    return x


def get_arg_type(fn, arg_position: int = 0):
    """
    Returns the type of the argument at position `arg_position` of `fn`.
    """
    sig = inspect.signature(fn)
    arg_type = sig.parameters[list(sig.parameters.keys())[arg_position]].annotation
    return arg_type


def create_kedro_conf_dirs(tmp_path: Path):
    config_path: Path = tmp_path / "conf" / "base"
    config_path.mkdir(parents=True)

    dummy_data_path = tmp_path / "file.txt"
    dummy_data_path.write_text(":)")

    with (config_path / "catalog.yml").open("wt") as f:
        yaml.safe_dump(
            {
                "input_data": {
                    "filepath": str(dummy_data_path.absolute()),
                    "type": "text.TextDataSet",
                },
            },
            f,
        )

    copy(
        Path(__file__).absolute().parent / "conf" / "local" / "snowflake.yml",
        config_path / "snowflake.yml",
    )

    return config_path


def has_any_calls_matching_predicate(
    mock: MagicMock, predicate: Callable[[Any], bool]
) -> bool:
    """
    Returns True if any call to `mock` matches `predicate`.
    """
    for call in mock.mock_calls:
        if predicate(call):
            return True
    return False
