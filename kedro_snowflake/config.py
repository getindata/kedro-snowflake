from typing import Optional, List

import yaml
from pydantic import BaseModel, root_validator, Field

KEDRO_SNOWFLAKE_CONFIG_PATTERN = "snowflake*"


class SnowflakeConnectionConfig(BaseModel):
    credentials: Optional[str]
    account: Optional[str]
    user: Optional[str]
    password_from_env: Optional[str]
    database: Optional[str]
    warehouse: Optional[str]
    schema_: Optional[str] = Field(alias="schema")
    role: Optional[str]

    @root_validator
    def check_credentials(cls, values):
        if not values.get("credentials") and not all(
            (
                values.get(field)
                for field in [
                    "account",
                    "user",
                    "password_from_env",
                    "database",
                    "warehouse",
                    "schema",
                    "role",
                ]
            )
        ):
            raise ValueError(
                "Either credentials or all of the following fields: account, user, "
                "password_from_env, database, warehouse, schema, role must be provided."
            )
        return values


class DependenciesConfig(BaseModel):
    packages: List[str] = [
        "snowflake-snowpark-python",
        "cachetools",
        "pluggy",
        "PyYAML==6.0",
        "jmespath",
        "click",
        "importlib_resources",
        "toml",
        "rich",
        "pathlib",
        "fsspec",
        "scikit-learn",
        "pandas",
        "zstandard",
        "more-itertools",
        "openpyxl",
        "backoff",
    ]
    imports: List[str] = [
        "kedro",
        "kedro_datasets",
        "kedro_snowflake",
        "omegaconf",
        "antlr4",
        "dynaconf",
        "anyconfig",
    ]


class SnowflakeRuntimeConfig(BaseModel):
    dependencies: DependenciesConfig
    stage: str = "@KEDRO_SNOWFLAKE_STAGE"
    temporary_stage: str = "@KEDRO_SNOWFLAKE_TEMP_DATA_STAGE"
    schedule: str = "11520 minute"
    stored_procedure_name_suffix: Optional[str] = ""


class SnowflakeConfig(BaseModel):
    connection: SnowflakeConnectionConfig
    runtime: SnowflakeRuntimeConfig


class KedroSnowflakeConfig(BaseModel):
    snowflake: SnowflakeConfig


CONFIG_TEMPLATE_YAML = """
snowflake:
  connection:
    # Either credentials name (Reference to a key in credentials.yml as in standard Kedro)
    # or leave 
    # credentials: ~
    # and specify rest of the fields
    credentials: ~
    account: "{account}"
    database: "{database}"
    # Name of the environment variable to take the Snowflake password from
    password_from_env: "{password_from_env}"
    role: "{role}"
    schema: "{schema}"
    user: "{user}"
    warehouse: "{warehouse}"
  runtime:
    # Default schedule for Kedro tasks
    schedule: "11520 minute"
    
    # Optional suffix for all kedro stored procedures
    stored_procedure_name_suffix: ""
    
    # Names of the stages
    # `stage` is for stored procedures etc.
    # `temporary_stage` is for temporary data serialization
    stage: "@KEDRO_SNOWFLAKE_STAGE"
    temporary_stage: '@KEDRO_SNOWFLAKE_TEMP_DATA_STAGE'
    
    # List of Python packages and imports to be used by the project
    # We recommend that this list will be add-only, and not modified
    # as it may break the project once deployed to Snowflake.
    # Modify at your own risk!
    dependencies:
      # imports will be taken from local environment and will get uploaded to Snowflake
      imports:
      - kedro
      - kedro_datasets
      - kedro_snowflake
      - omegaconf
      - antlr4
      - dynaconf
      - anyconfig
      # packages use official Snowflake's Conda Channel
      # https://repo.anaconda.com/pkgs/snowflake/
      packages:
      - snowflake-snowpark-python
      - cachetools
      - pluggy
      - PyYAML==6.0
      - jmespath
      - click
      - importlib_resources
      - toml
      - rich
      - pathlib
      - fsspec
      - scikit-learn
      - pandas
      - zstandard
      - more-itertools
      - openpyxl
      - backoff
""".strip()

# This auto-validates the template above during import
_CONFIG_TEMPLATE = KedroSnowflakeConfig.parse_obj(yaml.safe_load(CONFIG_TEMPLATE_YAML))
