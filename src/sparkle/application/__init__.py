import abc
import os
from typing import Any, Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from sparkle.config import Config, ExecutionEnvironment
from sparkle.writer import Writer
from sparkle.reader import Reader
from sparkle.utils.logger import logger


_SPARK_EXTENSIONS = [
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
]
_SPARK_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.apache.spark:spark-avro_2.12:3.3.0",
]


class Sparkle(abc.ABC):
    """Base class for Spark applications."""

    def __init__(
        self,
        env: ExecutionEnvironment,
        spark_config: Dict[str, Any],
        config: Config,
        writers: List[Writer],
    ):
        """Sparkle's application initializer.

        Args:
            env (ExecutionEnvironment): The environment in which the Spark job is running (LOCAL or AWS).
            spark_config (Dict[str, Any]): Dictionary containing additional Spark configuration settings.
            config (Config): Configuration object containing application-specific settings.
            writers (List[Writer]): List of Writer objects used for writing the processed data.
        """
        self.env = env
        self.spark_config = spark_config
        self.config = config
        self.writers = writers
        self.spark_session = self.get_spark_session(env)

    def get_spark_session(self, env: ExecutionEnvironment) -> SparkSession:
        """Create and return a Spark session based on the environment.

        Args:
            env (ExecutionEnvironment): The environment type (either LOCAL or AWS).

        Returns:
            SparkSession: The Spark session configured with the appropriate settings.
        """
        if env == ExecutionEnvironment.LOCAL:
            return self._get_local_session()
        elif env == ExecutionEnvironment.AWS:
            return self._get_aws_session()
        else:
            raise ValueError(f"Unsupported environment: {env}")

    def _get_local_session(self) -> SparkSession:
        """Create a Spark session for the local environment."""
        ivy_settings_path = os.environ.get("IVY_SETTINGS_PATH", None)
        LOCAL_CONFIG = {
            "spark.sql.extensions": ",".join(_SPARK_EXTENSIONS),
            "spark.jars.packages": ",".join(_SPARK_PACKAGES),
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": "./tmp/warehouse",
        }

        spark_conf = SparkConf()
        for key, value in LOCAL_CONFIG.items():
            spark_conf.set(key, str(value))
        for key, value in self.spark_config.items():
            spark_conf.set(key, str(value))

        spark_session_builder = SparkSession.builder.master("local[*]").appName(
            "LocalDataProductApp"
        )
        for key, value in spark_conf.getAll():
            spark_session_builder = spark_session_builder.config(key, value)

        return spark_session_builder.getOrCreate()

    def _get_aws_session(self) -> SparkSession:
        """Create a Spark session for the AWS environment."""
        try:
            from awsglue.context import GlueContext
        except ImportError:
            logger.error("Could not import GlueContext. Is this running on AWS Glue?")

        AWS_CONFIG = {
            "spark.sql.extensions": ",".join(_SPARK_EXTENSIONS),
            "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.glue_catalog.warehouse": "./tmp/warehouse",
        }

        spark_conf = SparkConf()
        for key, value in AWS_CONFIG.items():
            spark_conf.set(key, str(value))
        for key, value in self.spark_config.items():
            spark_conf.set(key, str(value))

        glue_context = GlueContext(SparkContext.getOrCreate(conf=spark_conf))
        return glue_context.spark_session

    @property
    def input(self) -> Dict[str, Reader]:
        """Dictionary of input DataReaders used in the application.

        Returns:
            dict[str, DataReader]: Dictionary of input DataReaders
              used in the application, keyed by source name.
        """
        if len(self.config.inputs) == 0:
            raise ValueError("No inputs configured.")

        return {
            key: value.with_config(self.config, self.spark_session)
            for key, value in self.config.inputs.items()
        }

    @abc.abstractmethod
    def process(self) -> DataFrame:
        """Application's entrypoint responsible for the main business logic.

        This method should be overridden in subclasses to process the
        input DataFrames and return a resulting DataFrame.

        Returns:
            DataFrame: The resulting DataFrame after processing.

        Raises:
            NotImplementedError: If the subclass does not implement
              this method.
        """
        raise NotImplementedError("process method must be implemented by subclasses")

    def write(self, df: DataFrame) -> None:
        """Write output DataFrame to the application's writer(s).

        The DataFrame is first persisted in memory to optimize writing
        operations and then unpersisted after all writers have
        completed their tasks.

        Args:
            df (DataFrame): The DataFrame to be written to the destinations.
        """
        df.persist(StorageLevel.MEMORY_ONLY)

        for writer in self.writers:
            writer.write(df)

        df.unpersist()
