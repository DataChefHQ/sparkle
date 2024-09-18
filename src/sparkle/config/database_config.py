from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    """Hive Table Configuration."""

    database: str
    table: str
