from kedro.framework.cli.starters import KedroStarterSpec

starters = [
    KedroStarterSpec(
        alias="snowflights",
        template_path="git+https://github.com/getindata/kedro-snowflake",
        directory="kedro_snowflake/starters/snowflights",
    )
]
# TODO - this shit does not work with Git, because Kedro Expects the git tag to be equal to Kedro version
