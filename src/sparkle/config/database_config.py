from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    """Hive Table Configuration."""

    bucket: str
    table: str
