
# Development

## Prerequisites
* poetry `1.3.2` (as of 2023-04-26)
* Python == 3.8
* Snowflake account (or trial)

## Local development
It's easiest to develop the plugin by having a side project created with Kedro (e.g. our Snowflights starter), managed by Poetry (since there is no `pip install -e` support in Poetry).
In the side project, just add the following entry in `pyproject.toml`:
```toml
[tool.poetry.dependencies]
kedro-snowflake = { path = "<full path to the plugin on local machine>", develop = true}
```
and invoke
```console
poetry update
poetry install
```
and all of the changes made in the plugin will be immediately visible in the side project (just as with `pip install -e` option).

From that point you can just use
```console
kedro snowflake run --wait-for-completion
```

to start the pipelines in Snowflake and develop new features/fix bugs.
