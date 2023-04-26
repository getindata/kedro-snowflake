# Kedro Snowflake Pipelines plugin

[![Python Version](https://img.shields.io/pypi/pyversions/kedro-snowflake)](https://github.com/getindata/kedro-snowflake)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/kedro-snowflake.svg)](https://pypi.org/project/kedro-snowflake/)
[![Downloads](https://pepy.tech/badge/kedro-snowflake)](https://pepy.tech/project/kedro-snowflake)

[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=getindata_kedro-snowflake&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=getindata_kedro-snowflake)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=getindata_kedro-snowflake&metric=coverage)](https://sonarcloud.io/summary/new_code?id=getindata_kedro-snowflake)
[![Documentation Status](https://readthedocs.org/projects/kedro-snowflake/badge/?version=latest)](https://kedro-snowflake.readthedocs.io/en/latest/?badge=latest)

<p align="center">
  <a href="https://getindata.com/solutions/ml-platform-machine-learning-reliable-explainable-feature-engineering"><img height="150" src="https://getindata.com/img/logo.svg"></a>
  <h3 align="center">We help companies turn their data into assets</h3>
</p>

## About
This plugin allows to run full Kedro pipelines in Snowflake. Right now it supports
* Kedro starter, to get you up to speed fast
* automatically creating Snowflake Stored Procedures from Kedro nodes (using Snowpark SDK)
* translating Kedro pipeline into Snowflake tasks graph
* running Kedro pipeline fully within Snowflake, without external system
* using Kedro's official `SnowparkTableDataSet`
* automatically storing intermediate data as Transient Tables (if Snowpark's DataFrames are used)

## Documentation
For detailed documentation refer to https://kedro-snowflake.readthedocs.io/

## Usage
### With starter
Create new project with our Kedro starter ❄️ _Snowflights_ 🚀:
```bash
kedro new --starter=snowflights --checkout=master
```