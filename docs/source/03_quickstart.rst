Quickstart
----------

Before you start, make sure that you have access to Snowflake account and prepare the following information:

-  Snowflake Username
-  Snowflake Password
-  Snowflake Account Name
-  Snowflake Warehouse Name
-  Snowflake Database Name
-  Snowflake Schema Name
-  Snowflake password (you will store it locally in an environment variable)

You will also need:

* Python 3.8 (must-have ⚠️ - this is enforced by the `snowflake-snowpark-python` package. Refer to `Snowflake documentation <https://docs.snowflake.com/en/developer-guide/snowpark/python/setup>`__ for more details.
* A tool to manage Python virtual environments (e.g. venv, conda, virtualenv). Anaconda is recommended by Snowflake.

-------

asd

1. Prepare new virtual environment with Python == 3.8. Install the packages


2. Install the plugin

.. code:: console

   pip install "kedro-snowflake>=0.1.0"


3. Create new project from our starter

.. code:: console

   kedro new --starter=snowflights

    Project Name
    ============
    Please enter a human readable name for your new project.
    Spaces, hyphens, and underscores are allowed.
     [Snowflights]:

    Snowflake Account
    =================
    Please enter the name of your Snowflake account.
    This is the part of the URL before .snowflakecomputing.com
     []: abc-123

    Snowflake User
    ==============
    Please enter the name of your Snowflake user.
     []: user2137

    Snowflake Warehouse
    ===================
    Please enter the name of your Snowflake warehouse.
     []: compute-wh

    Snowflake Database
    ==================
    Please enter the name of your Snowflake database.
     [KEDRO]:

    Snowflake Schema
    ================
    Please enter the name of your Snowflake schema.
     [PUBLIC]:

    Snowflake Password Environment Variable
    =======================================
    Please enter the name of the environment variable that contains your Snowflake password.
    Alternatively, you can re-configure the plugin later to use Kedro's credentials.yml
     [SNOWFLAKE_PASSWORD]:

    The project name 'Snowflights' has been applied to:
    - The project title in /tmp/snowflights/README.md
    - The folder created for your project in /tmp/snowflights
    - The project's python package in /tmp/snowflights/src/snowflights

3. Go to the project's directory: ``cd snowflights``
4. Install the requirements

.. code:: console

   pip install -r src/requirements.txt

7. Launch Kedro pipeline in Snowflake

.. code:: console

   kedro snowflake run --wait-for-completion

After launching the command, you will see auto-refreshing CLI interface, showing the progress of the tasks execution.

|Kedro Snowflake Pipeline execution|

.. |Kedro Snowflake Pipeline execution| image:: ../images/snowflake_running_pipeline.gif