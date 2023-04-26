from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_model_input_table,
    export_data_to_snowflake,
    preprocess_companies,
    preprocess_shuttles,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=export_data_to_snowflake,
                inputs="companies",
                outputs="companies_snowflake",
                name="export_data_to_snowflake_node",
            ),
            node(
                func=preprocess_companies,
                inputs="companies_snowflake",
                outputs="preprocessed_companies",
                name="preprocess_companies_node",
            ),
            node(
                func=preprocess_shuttles,
                inputs="shuttles",
                outputs="preprocessed_shuttles",
                name="preprocess_shuttles_node",
            ),
            node(
                func=create_model_input_table,
                inputs=["preprocessed_shuttles", "preprocessed_companies", "reviews"],
                outputs="model_input_table",
                name="create_model_input_table_node",
            ),
        ]
    )
