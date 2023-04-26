import datetime as dt
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable, List

from snowflake.snowpark import Session
from tabulate import tabulate

logger = logging.getLogger(__name__)


@dataclass
class KedroSnowflakePipeline:
    session: Session
    pipeline_tasks_sql: List[str]
    execute_sql: List[str]
    root_task_name: str
    pipeline_task_names: List[str]

    def run(
        self,
        wait_for_completion: bool = False,
        timeout_seconds: int = 600,
        echo_fn: Callable[[str], Any] = None,
        on_start_callback: Callable = None,
    ) -> bool:
        logger.info("Executing pipeline SQL")
        for sql in self.pipeline_tasks_sql + self.execute_sql:
            logger.debug(sql + os.linesep + os.linesep)
            self.session.sql(sql).collect()

        if on_start_callback:
            on_start_callback()

        if wait_for_completion:
            return self._wait_for_completion(echo_fn, timeout_seconds)
        else:
            return True

    def _wait_for_completion(self, echo_fn, timeout_seconds):
        echo = echo_fn or (lambda s: None)
        start_ts = monotonic()
        latest_task = self.session.sql(
            f"""
select name, run_id, scheduled_time, completed_time
from table(information_schema.task_history())
where scheduled_from = 'EXECUTE TASK' and name = '{self.root_task_name}'
order by scheduled_time desc
limit 1;""".strip()
        ).to_pandas()
        run_id = latest_task["RUN_ID"].values[0]
        finished = False

        def get_current_state():
            state = self.session.sql(
                f"""
select name, run_id, scheduled_time, completed_time, state
from table(information_schema.task_history())
where run_id = {run_id}
order by scheduled_time;
                            """.strip()
            ).to_pandas()
            return (
                state,
                tabulate(
                    state,
                    headers="keys",
                    tablefmt="psql",
                    showindex=False,
                ),
            )

        while not finished and (monotonic() - start_ts) < timeout_seconds:
            state, state_printable = get_current_state()
            echo(state_printable + os.linesep + f"Last update: {dt.datetime.now()}")
            task_names = set(state["NAME"].str.upper())
            finished = (
                all(state["COMPLETED_TIME"].notnull())
                and all(
                    t.upper() in task_names
                    for t in (self.pipeline_task_names + [self.root_task_name])
                )
            ) or any(state["STATE"].str.upper() == "FAILED")
            if finished:
                break
            sleep(11)
        else:
            echo(f"Pipeline timed out after {timeout_seconds}s")
            return False

        if finished:
            state, state_printable = get_current_state()
            success = all(state["STATE"].str.upper() == "SUCCEEDED")
            echo(
                state_printable
                + os.linesep
                + f"Pipeline finished at {dt.datetime.now()} (in approx. {monotonic()-start_ts}s). "
                f"Status: {'SUCCEEDED' if success else 'FAILED'}"
            )

            return success
        else:
            return False

    def save(self, path: Path):
        path.write_text("\n\n".join(self.pipeline_tasks_sql + self.execute_sql))
