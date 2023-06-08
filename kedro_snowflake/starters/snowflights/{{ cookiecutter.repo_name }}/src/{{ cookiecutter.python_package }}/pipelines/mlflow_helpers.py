import json
import logging
from typing import Any, Dict

import mlflow
import pandas as pd
import snowflake.snowpark as sp
from sklearn.ensemble import RandomForestRegressor

from kedro_snowflake.config import SnowflakeMLflowConfig

log = logging.getLogger(__name__)


def _get_current_session():
    return sp.context.get_active_session()


def _get_mlflow_config():
    session = _get_current_session()
    # FIXME: hardcoded task name!
    json_obj = json.loads(session.sql(
        "call system$get_predecessor_return_value('KEDRO_SNOWFLAKE_MLFLOW_START_DEFAULT_TASK')").collect()[
                              0][0])
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


def log_model(model: RandomForestRegressor, X_train: pd.DataFrame, y_train: pd.Series):
    mlflow.set_tracking_uri("file:///tmp/mlruns")
    mlflow_config = _get_mlflow_config()
    local_exp_id = mlflow.create_experiment("temp")
    exp_id = _get_experiment_id()
    run_id = _get_run_id()
    with mlflow.start_run(run_name='local', experiment_id=local_exp_id) as run:
        local_run_id = run.info.run_id
        mlflow.sklearn.autolog()
        model.fit(X_train, y_train)
        session = _get_current_session()
        session.file.put(f"/tmp/mlruns/{local_exp_id}/{local_run_id}/artifacts/model/*",
                         f"{mlflow_config.stage}/{exp_id}/{run_id}/artifacts/model/",
                         auto_compress=False)
        return model
