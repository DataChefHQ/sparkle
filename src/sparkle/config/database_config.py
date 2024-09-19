from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    """Hive Table Configuration."""

    database: str
    table: str
    bucket: str
    catalog_name: str = "glue_catalog"
    catalog_id: str | None = None
