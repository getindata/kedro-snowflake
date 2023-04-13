import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


@dataclass
class KedroSnowflakePipeline:
    session: Session
    pipeline_tasks_sql: List[str]
    execute_sql: List[str]

    def run(self):
        logger.info("Executing pipeline SQL")
        for sql in self.pipeline_tasks_sql + self.execute_sql:
            logger.debug(sql + os.linesep + os.linesep)
            self.session.sql(sql).collect()

    def save(self, path: Path):
        path.write_text("\n\n".join(self.pipeline_tasks_sql + self.execute_sql))
