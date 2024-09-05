from dataclasses import dataclass
from .kafka_config import KafkaConfig
from .iceberg_config import IcebergConfig
from .database_config import TableConfig


@dataclass(frozen=True)
class Config:
    """Sparkle Application Configuration."""

    app_name: str
    app_id: str
    version: str
    database_bucket: str
    kafka: KafkaConfig | None
    hive_table_input: TableConfig | None
    iceberg_output_config: IcebergConfig | None
    spark_trigger: str = '{"once": True}'
