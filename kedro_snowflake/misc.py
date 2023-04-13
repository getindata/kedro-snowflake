from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

from snowflake.snowpark import Session


@dataclass
class CliContext:
    env: str
    metadata: Any


@dataclass
class KedroSnowflakePipeline:
    session: Session
    pipeline_tasks_sql: List[str]
    execute_sql: List[str]

    def run(self):
        for sql in self.pipeline_tasks_sql:
            self.session.sql(sql)
        for sql in self.execute_sql:
            self.session.sql(sql)

    def save(self, path: Path):
        path.write_text("\n\n".join(self.pipeline_tasks_sql + self.execute_sql))
