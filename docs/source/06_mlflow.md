# [Beta] MLflow support

## High level architecture

![MLflow and Kedro-snowflake](../images/mlflow-support.png)

## Configuration example

```yaml
  mlflow:
    # MLflow experiment name for tracking runs
    experiment_name: demo-mlops
    stage: "@MLFLOW_STAGE"
    # Snowflake external functions needed for calling MLflow instance
    functions:
      experiment_get_by_name: demo.demo.mlflow_experiment_get_by_name
      run_create: demo.demo.mlflow_run_create
      run_log_metric: demo.demo.mlflow_run_log_metric
      run_log_parameter: demo.demo.mlflow_run_log_parameter

```
