from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IcebergConfig:
    """Iceberg Configuration."""

    compact: bool = True
    expire_snapshots: bool = True
    rewrite_manifest: bool = True
    compaction_strategy: str = "binpack"
    compaction_options: dict[str, Any] | None = None
    sort_order: str | None = None
    snapshot_max_age_days: int = 14
    min_snapshots: int = 50
