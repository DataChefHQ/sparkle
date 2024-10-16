from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column


@dataclass(frozen=True)
class IcebergConfig:
    """Iceberg Configuration."""

    database_name: str
    database_path: str
    table_name: str
    delete_before_write: bool = False
    catalog_name: str = "glue_catalog"
    catalog_id: str | None = None
    partitions: list[Column] | None = None
    number_of_partitions: int = 1

    compact: bool = True
    expire_snapshots: bool = True
    rewrite_manifest: bool = True
    compaction_strategy: str = "binpack"
    compaction_options: dict[str, Any] | None = None
    sort_order: str | None = None
    snapshot_max_age_days: int = 14
    min_snapshots: int = 50
