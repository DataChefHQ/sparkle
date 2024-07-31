from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableConfig:
    """Hive Table Configuration."""

    bucket: str
    table: str


@dataclass(frozen=True)
class Credentials:
    """Credentials convention to use for any external services.

    Args:
        username (str): Username.
        password (str): Password.

    Note: the password is stored as a string. Don't log it.
    """

    username: str | None
    password: str | None


@dataclass(frozen=True)
class SchemaRegistryConfig:
    """Schema Registry Configuration."""

    url: str
    credentials: Credentials


@dataclass
class KafkaConfig(frozen=True):
    """Kafka Configuration."""

    bootstrap_servers: str
    credentials: Credentials
    checkpoints_bucket: str
    starting_offset: str = "earliest"
    auth_protocol: str = "SASL_SSL"
    auth_mechanism: str = "PLAIN"
    schema_registry: SchemaRegistryConfig | None


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


@dataclass(frozen=True)
class SparkConfig:
    """Spark Application Configuration."""

    app_name: str
    app_id: str
    version: str
    database_bucket: str
    kafka: KafkaConfig | None
    input_database: TableConfig | None
    output_database: TableConfig | None
    spark_trigger: str = {"once": True}
    iceberg_config: IcebergConfig | None
