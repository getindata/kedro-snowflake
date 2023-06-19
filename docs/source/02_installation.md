# Installation guide

## Prerequisites

* Python 3.8 is a must ⚠️ - this is enforced by the `snowflake-snowpark-python` package. Refer to [Snowflake documentation](https://docs.snowflake.com/en/developer-guide/snowpark/python/setup) for more details. 
* A tool to manage Python virtual environments (e.g. venv, conda, virtualenv). Anaconda is recommended by Snowflake.
* Kedro is fixed for now at version `<0.18.9` due to data set errors that appear in later versions.

## Plugin installation

### Install from PyPI

Install the plugin (it automatically installs Kedro in a supported version)

```console
$ pip install "kedro-snowflake>=0.1.0"
```

### Install from sources

You may want to install the develop branch which has unreleased features:

```console
pip install git+https://github.com/getindata/kedro-snowflake.git@develop
```

## Available commands

You can check available commands by going into project directory and running:

```console
kedro snowflake
kedro snowflake [OPTIONS] COMMAND [ARGS]...

Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  init  Creates basic configuration for Kedro Snowflake plugin
  run   Runs the pipeline using Snowflake Tasks
```
