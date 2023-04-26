from dataclasses import dataclass
from typing import Any


@dataclass
class CliContext:
    env: str
    metadata: Any
