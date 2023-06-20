{% if not cookiecutter.enable_mlflow_integration|lower != "false" %}
# This file is empty, because mlflow integration was turned off at starter creation
{% else %}
import os
import json
import logging
from typing import Any, Dict

import mlflow
import pandas as pd
import snowflake.snowpark as sp
from kedro_snowflake.config import SnowflakeMLflowConfig

log = logging.getLogger(__name__)


def _get_current_session():
    return sp.context.get_active_session()


def _get_mlflow_config():
    def error_out():
        log.error("""SNOWFLAKE_MLFLOW_CONFIG environment variable not valid. 
Seems like something went wrong or you tried to run the mlflow-integration enabled snowflights starter pipeline locally, which is currently not supported, as mlflow_helpers functions are made for snowflake-mlflow integration.""")
        exit(1)
    try:
        json_obj = json.loads(os.environ.get("SNOWFLAKE_MLFLOW_CONFIG"))
        mlflow_config = SnowflakeMLflowConfig.parse_obj(json_obj)
    except TypeError:
        error_out()
    except json.JSONDecodeError:
        error_out()
    return mlflow_config


def _get_run_id():
    return _get_mlflow_config().run_id


def _get_experiment_id():
    session = _get_current_session()
    mlflow_config = _get_mlflow_config()
    return eval(session.sql(
        f"SELECT {mlflow_config.functions.experiment_get_by_name}('{mlflow_config.experiment_name}'):body.experiments[0].experiment_id").collect()[
                    0][0])


def get_snowflake_account():
    session = _get_current_session()
    return eval(session.get_current_account())


def get_current_warehouse():
    session = _get_current_session()
    return eval(session.get_current_warehouse())


def log_metric(key: str, val: float):
    session = _get_current_session()
    mlflow_config = _get_mlflow_config()
    run_id = _get_run_id()
    session.sql(
        f"select {mlflow_config.functions.run_log_metric}('{run_id}', '{key}', {val})").collect()


def log_parameter(key: str, val: Any):
    session = _get_current_session()
    mlflow_config = _get_mlflow_config()
    run_id = _get_run_id()
    val_str = str(val)
    try:
        session.sql(
            f"select {mlflow_config.functions.run_log_parameter}('{run_id}', '{key}', '{val_str}')").collect()
    except Exception as e:
        log.warning(f"Param {key} contains unsupported value {val_str}")


def log_params(params: Dict[str, Any]):
    for k, v in params.items():
        log_parameter(k, v)

class ModelLoggingContext():
    def __init__(self):
        mlflow.set_tracking_uri("file:///tmp/mlruns")
        self.mlflow_config = _get_mlflow_config()
        self.exp_id = _get_experiment_id()
        self.run_id = _get_run_id()

    def __enter__(self):
        self.local_exp_id = mlflow.create_experiment("temp")
        self.mlflow_run = mlflow.start_run(run_name='local', experiment_id=self.local_exp_id).__enter__()
        self.local_run_id = self.mlflow_run.info.run_id
        mlflow.sklearn.autolog()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        session = _get_current_session()
        session.file.put(f"/tmp/mlruns/{self.local_exp_id}/{self.local_run_id}/artifacts/model/*",
                        f"{self.mlflow_config.stage}/{self.exp_id}/{self.run_id}/artifacts/model/",
                        auto_compress=False)
        self.mlflow_run.__exit__(exc_type, exc, exc_tb)

def log_model() -> ModelLoggingContext:
    return ModelLoggingContext()


def run_update(status: str):
    session = _get_current_session()
    mlflow_config = _get_mlflow_config()
    run_id = _get_run_id()
    session.sql(
        f"select {mlflow_config.functions.run_update}('{run_id}', '{status}')").collect()    
{% endif %}