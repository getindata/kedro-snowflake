from kedro.framework.cli.starters import KedroStarterSpec

starters = [
    KedroStarterSpec(
        alias="snowflights",
        template_path="git+https://github.com/getindata/kedro-snowflake",
        directory="kedro_snowflake/starters/snowflights",
    )
]
