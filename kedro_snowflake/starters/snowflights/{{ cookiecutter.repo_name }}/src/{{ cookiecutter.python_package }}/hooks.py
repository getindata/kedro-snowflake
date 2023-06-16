from typing import Any, Dict

from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node

{% if cookiecutter.enable_mlflow_integration != "False" %}
from .pipelines import mlflow_helpers
{% endif %}

class SnowflakeHook:
    def __init__(self):
        self.run_params = None

    @hook_impl
    def before_pipeline_run(self, run_params: Dict[str, Any]) -> None:
        """Hook implementation to start an MLflow run
        with the session_id of the Kedro pipeline run.
        """
        self.run_params = run_params

{% if cookiecutter.enable_mlflow_integration != "False" %}
    @hook_impl
    def before_node_run(self, node: Node) -> None:
        if node.name == "export_data_to_snowflake_node":
            mlflow_helpers.log_params(self.run_params)

    @hook_impl
    def after_node_run(self, node: Node) -> None:
        if node.name == "evaluate_model_node":
            mlflow_helpers.run_update("FINISHED")
{% endif %}