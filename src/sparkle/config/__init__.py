from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
import os
from sparkle.reader import Reader
from .kafka_config import KafkaReaderConfig, KafkaWriterConfig
from .iceberg_config import IcebergConfig
from .database_config import TableConfig


class ExecutionEnvironment(Enum):
    """Enum for defining the execution environment."""

    LOCAL = "LOCAL"
    AWS = "AWS"


@dataclass(frozen=True)
class Config:
    """Sparkle Application Configuration.

    Attributes:
        app_name (str): The name of the application.
        app_id (str): The ID of the application.
        version (str): The version of the application.
        database_bucket (str): The S3 bucket where the database is stored.
        checkpoints_bucket (str): The S3 bucket where the Spark checkpoints are stored.
        inputs (dict[str, Type[Reader]]): A dictionary mapping input sources to Reader types.
        execution_environment (ExecutionEnvironment): The environment where the app is executed.
        filesystem_scheme (str): The file system scheme, default is 's3a://'.
        spark_trigger (str): The Spark trigger configuration in JSON format.
        kafka_input (KafkaReaderConfig, optional): Configuration for Kafka input.
        kafka_output (KafkaWriterConfig, optional): Configuration for Kafka output.
        hive_table_input (TableConfig, optional): Configuration for Hive table input.
        iceberg_output (IcebergConfig, optional): Configuration for Iceberg output.
    """

    app_name: str
    app_id: str
    version: str
    database_bucket: str
    checkpoints_bucket: str
    inputs: dict[str, type[Reader]]
    execution_environment: ExecutionEnvironment = ExecutionEnvironment.LOCAL
    filesystem_scheme: str = "s3a://"
    spark_trigger: str = '{"once": True}'
    kafka_input: KafkaReaderConfig | None = None
    kafka_output: KafkaWriterConfig | None = None
    hive_table_input: TableConfig | None = None
    iceberg_output: IcebergConfig | None = None

    @property
    def checkpoint_location(self) -> str:
        """Path to the S3 location where checkpoints are stored.

        This path is constructed based on the application ID, Kafka input and output configurations,
        and the checkpoints bucket.

        Returns:
            str: The full path to the checkpoint location.

        Raises:
            ValueError: If Kafka input or output configurations are missing.
        """
        if not self.kafka_input or not self.kafka_output:
            raise ValueError("Kafka input and output configurations are required.")

        return os.path.join(
            f"{self.filesystem_scheme}{self.checkpoints_bucket}",
            "spark",
            self.app_id,
            self.kafka_input.kafka_topic,
            self.kafka_output.kafka_topic,
        )

    @staticmethod
    def get_local_spark_config(
        extra_config: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Provides local Spark configurations and allows merging with additional configurations.

        Args:
            extra_config (dict[str, str], optional): Additional configurations to merge with the default ones.

        Returns:
            dict[str, str]: dictionary of Spark configurations for the local environment.
        """
        default_config = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.apache.spark:spark-avro_2.12:3.3.0",
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": "./tmp/warehouse",
        }
        if extra_config:
            default_config.update(extra_config)
        return default_config

    @staticmethod
    def get_aws_spark_config(
        extra_config: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Provides AWS-specific Spark configurations and allows merging with additional configurations.

        Args:
            extra_config (dict[str, str], optional): Additional configurations to merge with the default ones.

        Returns:
            dict[str, str]: dictionary of Spark configurations for the AWS environment.
        """
        default_config = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.glue_catalog.warehouse": "./tmp/warehouse",
        }
        if extra_config:
            default_config.update(extra_config)
        return default_config

    @staticmethod
    def get_spark_extensions(extra_config: list[str] | None = None) -> list[str]:
        """Provides Spark session extensions and allows merging with additional extensions.

        Args:
            extra_config (list[str], optional): Additional extensions to merge with the default ones.

        Returns:
            list[str]: A list of Spark session extensions.
        """
        default_extensions = [
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ]
        if extra_config:
            default_extensions.extend(extra_config)
        return default_extensions

    @staticmethod
    def get_spark_packages(extra_config: list[str] | None = None) -> list[str]:
        """Provides Spark packages and allows merging with additional packages.

        Args:
            extra_config (list[str], optional): Additional packages to merge with the default ones.

        Returns:
            list[str]: A list of Spark packages.
        """
        default_packages = [
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.apache.spark:spark-avro_2.12:3.3.0",
        ]
        if extra_config:
            default_packages.extend(extra_config)
        return default_packages

    @staticmethod
    def get_spark_config(
        env: ExecutionEnvironment, extra_config: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Gets the appropriate Spark configurations based on the environment.

        Args:
            env (ExecutionEnvironment): The environment type (either LOCAL or AWS).
            extra_config (dict[str, str], optional): Additional configurations to merge with the default ones.

        Returns:
            dict[str, str]: The Spark configurations for the given environment.

        Raises:
            ValueError: If an unsupported environment is provided.
        """
        if env == ExecutionEnvironment.LOCAL:
            return Config.get_local_spark_config(extra_config)
        elif env == ExecutionEnvironment.AWS:
            return Config.get_aws_spark_config(extra_config)
        else:
            raise ValueError(f"Unsupported environment: {env}")
