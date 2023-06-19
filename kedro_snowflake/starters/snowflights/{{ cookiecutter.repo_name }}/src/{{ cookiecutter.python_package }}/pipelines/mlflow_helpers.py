import os

{% if not cookiecutter.enable_mlflow_integration != "False" %}
# This file is empty, because mlflow integration was turned off at starter creation
{% else %}
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
    json_obj = json.loads(os.environ.get("SNOWFLAKE_MLFLOW_CONFIG"))
    mlflow_config = SnowflakeMLflowConfig.parse_obj(json_obj)
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


def log_model(model, x_train: pd.DataFrame, y_train: pd.Series):
    mlflow.set_tracking_uri("file:///tmp/mlruns")
    mlflow_config = _get_mlflow_config()
    local_exp_id = mlflow.create_experiment("temp")
    exp_id = _get_experiment_id()
    run_id = _get_run_id()
    with mlflow.start_run(run_name='local', experiment_id=local_exp_id) as run:
        local_run_id = run.info.run_id
        mlflow.sklearn.autolog()
        model.fit(x_train, y_train)
        session = _get_current_session()
        session.file.put(f"/tmp/mlruns/{local_exp_id}/{local_run_id}/artifacts/model/*",
                         f"{mlflow_config.stage}/{exp_id}/{run_id}/artifacts/model/",
                         auto_compress=False)
        return model


def run_update(status: str):
    session = _get_current_session()
    mlflow_config = _get_mlflow_config()
    run_id = _get_run_id()
    session.sql(
        f"select {mlflow_config.functions.run_update}('{run_id}', '{status}')").collect()    
{% endif %}