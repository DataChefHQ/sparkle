from dataclasses import dataclass
from enum import Enum
from sparkle.reader import Reader
from .kafka_config import KafkaReaderConfig, KafkaWriterConfig
from .iceberg_config import IcebergConfig
from .database_config import TableConfig
import os
from typing import Type


class ExecutionEnvironment(Enum):
    LOCAL = "LOCAL"
    AWS = "AWS"


@dataclass(frozen=True)
class Config:
    """Sparkle Application Configuration."""

    app_name: str
    app_id: str
    version: str
    database_bucket: str
    checkpoints_bucket: str
    filesystem_scheme: str = "s3a://"
    spark_trigger: str = '{"once": True}'
    kafka_input: KafkaReaderConfig | None = None
    kafka_output: KafkaWriterConfig | None = None
    hive_table_input: TableConfig | None = None
    iceberg_output: IcebergConfig | None = None

    inputs: dict[str, Type[Reader]] = {}

    @property
    def checkpoint_location(self):
        """Path to the s3 location where checkpoints are stored."""
        if not self.kafka_input or not self.kafka_output:
            raise ValueError("Kafka input and output configurations are required.")

        return os.path.join(
            f"{self.filesystem_scheme}{self.checkpoints_bucket}",
            "spark",
            self.app_id,
            self.kafka_input.kafka_topic,
            self.kafka_output.kafka_topic,
        )
