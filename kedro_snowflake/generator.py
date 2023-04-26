import logging
import os
import re
import tempfile
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, List, Optional

from kedro.pipeline import Pipeline
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.session import Session

from kedro_snowflake.config import KedroSnowflakeConfig
from kedro_snowflake.pipeline import KedroSnowflakePipeline
from kedro_snowflake.utils import (
    get_module_path,
    zip_dependencies,
    zstd_folder,
)

logger = logging.getLogger(__name__)

PARAMS_PREFIX = "params:"


class SnowflakePipelineGenerator:
    SPROC_NAME = "RUN_KEDRO"
    TASK_TEMPLATE = """
create or replace task {task_name}
warehouse = '{warehouse}'
after {after_tasks}
as
{task_body};
""".strip()
    TASK_BODY_TEMPLATE = """
call {sproc_name}('{environment}', system$get_predecessor_return_value('{root_task_name}'), '{pipeline_name}', ARRAY_CONSTRUCT({nodes_to_run}), '{extra_params}')
""".strip()  # noqa: E501

    def __init__(
        self,
        pipeline_name: str,
        kedro_environment: str,
        config: KedroSnowflakeConfig,
        connection_parameters: Dict[str, Any],
        kedro_params: Dict[str, Any],
        extra_params: Optional[str] = None,
        extra_env: Dict[str, str] = None,
    ):
        assert all(
            k in connection_parameters
            for k in ("account", "user", "password", "warehouse", "database", "schema")
        ), "Missing one or more connection parameters"

        self.connection_parameters = connection_parameters
        self.kedro_environment = kedro_environment

        self.extra_params = extra_params
        self.kedro_params = kedro_params
        self.config = config
        self.pipeline_name = pipeline_name
        self.extra_env = extra_env

    def _get_pipeline_name_for_snowflake(self):
        return (self.config.snowflake.runtime.pipeline_name_mapping or {}).get(
            self.pipeline_name, self.pipeline_name
        )

    def _generate_task_sql(
        self,
        task_name: str,
        after_tasks: List[str],
        pipeline_name: str,
        nodes_to_run: List[str],
        extra_params: Optional[str] = None,
    ):
        return self.TASK_TEMPLATE.format(
            task_name=task_name,
            warehouse=self.connection_parameters["warehouse"],
            after_tasks=",".join(after_tasks),
            task_body=self.TASK_BODY_TEMPLATE.format(
                root_task_name=self._root_task_name,
                environment=self.kedro_environment,
                sproc_name=self.SPROC_NAME,
                pipeline_name=pipeline_name,
                nodes_to_run=",".join(f"'{n}'" for n in nodes_to_run),
                extra_params=extra_params or "",
            ),
        )

    def _generate_root_task_sql(self):
        return """
create or replace task {task_name}
warehouse = '{warehouse}'
schedule = '{schedule}'
as
call {root_sproc}();
""".strip().format(
            task_name=self._root_task_name,
            warehouse=self.connection_parameters["warehouse"],
            root_sproc=self._root_sproc_name,
            schedule=self.config.snowflake.runtime.schedule,
        )

    def _sanitize_node_name(self, node_name: str) -> str:
        return re.sub(r"\W", "_", node_name)

    def _generate_snowflake_tasks_sql(
        self,
        pipeline: Pipeline,
    ) -> List[str]:
        sql_statements = [self._generate_root_task_sql()]

        node_dependencies = (
            pipeline.node_dependencies
        )  # <-- this one is not topological
        for node in pipeline.nodes:  # <-- this one is topological
            after_tasks = [self._root_task_name] + [
                self._sanitize_node_name(n.name) for n in node_dependencies[node]
            ]
            sql_statements.append(
                self._generate_task_sql(
                    self._sanitize_node_name(node.name),
                    after_tasks,
                    self.pipeline_name,
                    [node.name],
                    self.extra_params,
                )
            )
        return sql_statements

    def _generate_task_execute_sql(self):
        return [
            f"call SYSTEM$TASK_DEPENDENTS_ENABLE( '{self._root_task_name}' );",
            f"alter task {self._root_task_name} resume;",
            f"execute task {self._root_task_name};",
        ]

    @property
    def _root_task_name(self):
        root_task_name = f"kedro_snowflake_start_{self._get_pipeline_name_for_snowflake()}_task".upper()
        return root_task_name

    @property
    def _root_sproc_name(self):
        return (
            f"kedro_snowflake_start_{self._get_pipeline_name_for_snowflake()}".upper()
        )

    def generate(self) -> KedroSnowflakePipeline:
        """Generate a SnowflakePipeline object from a Kedro pipeline.
        It can be used to run the pipeline or just to get the SQL statements.
        Note that the pipeline is not executed, it is just translated to SQL statements,
        but the stored procedures ARE created.
        """

        pipeline = self.get_kedro_pipeline()

        logger.info(f"Translating {self.pipeline_name} to Snowflake Pipeline")

        snowflake_stage_name = self.config.snowflake.runtime.stage
        snowflake_temp_data_stage = self.config.snowflake.runtime.temporary_stage
        session = self.snowflake_session
        self._drop_and_recreate_stages(snowflake_stage_name, snowflake_temp_data_stage)

        # TODO - groups -> nodes operating on sp.DataFrames could be merged (or use Kedro tags)
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            dependencies_dir = tmp_dir / "dependencies"
            dependencies_dir.mkdir()

            project_files_dir = tmp_dir / "project"
            project_files_dir.mkdir()

            # Package dependencies, based on config + hardcoded libs
            self._package_dependencies(dependencies_dir, project_files_dir)

            # Package this project
            self._package_kedro_project(project_files_dir)

            # Stage the packages - upload to Snowflake
            logger.info("Uploading dependencies to Snowflake")
            session.file.put(
                str(dependencies_dir / "*"),
                snowflake_stage_name,
                overwrite=True,
                auto_compress=False,
                parallel=8,
            )

            # Stage Kedro and this project
            logger.info("Uploading project files & special dependencies to Snowflake")
            session.file.put(
                str(project_files_dir / "*"),
                f"{snowflake_stage_name}/project",
                overwrite=True,
                auto_compress=False,
                parallel=8,
            )

            logger.info("Creating Kedro Snowflake root sproc")
            root_sproc = self._construct_kedro_snowflake_root_sproc(  # noqa: F841
                snowflake_stage_name
            )

            logger.info("Creating Kedro Snowflake Sproc")
            snowflake_sproc = self._construct_kedro_snowflake_sproc(
                imports=self._generate_imports_for_sproc(
                    dependencies_dir, snowflake_stage_name
                ),
                packages=self.config.snowflake.runtime.dependencies.packages,
                stage_location=snowflake_stage_name,
                temp_data_stage=snowflake_temp_data_stage,
            )

            logger.debug(snowflake_sproc)
            pipeline_sql_statements = self._generate_snowflake_tasks_sql(pipeline)
            return KedroSnowflakePipeline(
                session,
                pipeline_sql_statements,
                self._generate_task_execute_sql(),
                self._root_task_name,
                [self._sanitize_node_name(n.name) for n in pipeline.nodes],
            )

    def _generate_imports_for_sproc(self, dependencies_dir, snowflake_stage_name):
        imports_for_sproc = [
            f"{snowflake_stage_name}/{f.name}"
            for f in dependencies_dir.glob("*")
            if f.is_file()
        ]
        return imports_for_sproc

    def _package_kedro_project(self, project_files_dir):
        # TODO - handle ignoring some files (data etc)
        project_package_name = f"{self.pipeline_name}.tar.zst"
        zstd_folder(
            Path.cwd(),
            project_files_dir,
            file_name=project_package_name,
            exclude=[".pyc", "__pycache__"],
        )

    def _package_dependencies(self, dependencies_dir, project_files_dir):
        # Package dependencies that work with Snowpark's import
        zip_dependencies(
            [
                "toposort",
            ],
            dependencies_dir,
        )
        # Special packages that need to be extracted into PYTHONPATH at runtime (imports don't work)
        special_packages = self.config.snowflake.runtime.dependencies.imports
        for sp in special_packages:
            zstd_folder(
                get_module_path(sp),
                project_files_dir,
                file_name=f"{sp}.tar.zst",
                exclude=[".pyc", "__pycache__"],
            )

    def _drop_and_recreate_stages(self, *stages):
        # Enforce clean stage
        for s in stages:
            self.snowflake_session.sql(
                f"drop stage if exists {s.lstrip('@')};"
            ).collect()
            self.snowflake_session.sql(f"create stage {s.lstrip('@')};").collect()

    @cached_property
    def snowflake_session(self):
        return Session.builder.configs(self.connection_parameters).create()

    def _construct_kedro_snowflake_root_sproc(self, stage_location: str):
        def kedro_start_run(session: Session) -> str:
            from uuid import uuid4

            run_id = uuid4().hex
            session.sql(f"call system$set_return_value('{run_id}');").collect()
            return run_id

        return sproc(
            func=kedro_start_run,
            name=self._root_sproc_name,
            is_permanent=True,
            replace=True,
            stage_location=stage_location,
            packages=["snowflake-snowpark-python"],
            execute_as="caller",
            session=self.snowflake_session,
        )

    def _construct_kedro_snowflake_sproc(
        self,
        imports: List[str],
        packages: List[str],
        stage_location: str,
        temp_data_stage: str,
    ):
        # create a Snowpark Stored Procedure from Kedro node (node arg)
        # and return it

        project_name = Path.cwd().name

        def kedro_sproc_executor(
            session: Session,
            environment: str,
            run_id: str,
            pipeline_name: str,
            node_names: Optional[List[str]],
            extra_params_json: str,
        ) -> str:
            import json
            import sys
            import tarfile
            from pathlib import Path
            from time import monotonic

            import zstandard as zstd

            # This might be useful in near future ;)
            # IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
            # import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

            execution_data = {
                "environment": environment,
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "node_names": node_names,
                "extra_params_json": extra_params_json,
            }

            def extract_tar_zstd(input_path, output_path):
                with zstd.open(input_path, "rb") as archive:
                    tarfile.open(fileobj=archive, mode="r|").extractall(output_path)

            # Extract project and special dependencies
            extract_start_ts = monotonic()
            project_file_list = [
                file[0]
                for file in session.sql(f"LS {stage_location}/project").collect()
            ]
            for file in project_file_list:
                session.file.get(f"@{file}", "/tmp")

            for archive in Path("/tmp").glob("*.tar.zst"):
                extract_tar_zstd(archive, "/tmp")

            execution_data["extract_time"] = monotonic() - extract_start_ts

            # Add kedro to python path
            sys.path.insert(0, "/tmp/")

            # Run Kedro project
            kedro_init_start_ts = monotonic()
            from kedro.framework import session as k_session
            from kedro.framework.startup import bootstrap_project

            # apply patch to avoid calling subprocess
            def patch(*args):
                return {}

            k_session.session._describe_git = patch

            project_root = Path("/tmp") / project_name
            # return str(project_root)
            os.chdir(project_root)
            bootstrap_project(project_root)

            from kedro_snowflake.runner import SnowflakeRunner

            execution_data["kedro_init_time"] = monotonic() - kedro_init_start_ts
            kedro_run_start_ts = monotonic()

            with k_session.KedroSession.create(
                project_path=project_root,
                env=environment,
                extra_params=(
                    json.loads(extra_params_json) if extra_params_json else None
                ),
            ) as kedro_session:
                kedro_session.run(
                    pipeline_name,
                    node_names=node_names if node_names else None,
                    runner=SnowflakeRunner(session, temp_data_stage, run_id),
                )

            execution_data["kedro_run_time"] = monotonic() - kedro_run_start_ts
            return json.dumps(execution_data)

        node_sproc = sproc(
            func=kedro_sproc_executor,
            name=self.SPROC_NAME,  # TODO - make this dynamic or from config
            is_permanent=True,
            replace=True,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            execute_as="caller",
            session=self.snowflake_session,
        )
        return node_sproc

    def get_kedro_pipeline(self) -> Pipeline:
        from kedro.framework.project import pipelines

        pipeline: Pipeline = pipelines[self.pipeline_name]
        return pipeline
