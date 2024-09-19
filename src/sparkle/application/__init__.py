import abc
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf, SparkContext
from sparkle.config import Config, ExecutionEnvironment
from sparkle.writer import Writer
from sparkle.reader import Reader
from sparkle.utils.logger import logger

PROCESS_TIME_COLUMN = "process_time"


class Sparkle(abc.ABC):
    """Base class for Spark applications."""

    def __init__(
        self,
        config: Config,
        writers: list[Writer],
        spark_extensions: list[str] | None = None,
        spark_packages: list[str] | None = None,
        extra_spark_config: dict[str, str] | None = None,
    ):
        """Sparkle's application initializer.

        Args:
            config (Config): Configuration object containing application-specific settings.
            writers (list[Writer]): list of Writer objects used for writing the processed data.
            spark_extensions (list[str], optional): list of Spark session extensions to use.
            spark_packages (list[str], optional): list of Spark packages to include.
            extra_spark_config (dict[str, str], optional): Additional Spark configurations
              to merge with default configurations.
        """
        self.config = config
        self.writers = writers
        self.execution_env = config.execution_environment
        self.spark_config = config.get_spark_config(
            self.execution_env, extra_spark_config
        )
        self.spark_extensions = config.get_spark_extensions(spark_extensions)
        self.spark_packages = config.get_spark_packages(spark_packages)

        self.spark_session = self.get_spark_session(self.execution_env)

    def get_spark_session(self, env: ExecutionEnvironment) -> SparkSession:
        """Create and return a Spark session based on the environment.

        Args:
            env (ExecutionEnvironment): The environment type (either LOCAL or AWS).

        Returns:
            SparkSession: The Spark session configured with the appropriate settings.

        Raises:
            ValueError: If the environment is unsupported.
        """
        if env == ExecutionEnvironment.LOCAL:
            return self._get_local_session()
        elif env == ExecutionEnvironment.AWS:
            return self._get_aws_session()
        else:
            raise ValueError(f"Unsupported environment: {env}")

    def _get_local_session(self) -> SparkSession:
        """Create a Spark session for the local environment.

        Returns:
            SparkSession: Configured Spark session for local environment.
        """
        spark_conf = SparkConf()
        for key, value in self.spark_config.items():
            spark_conf.set(key, str(value))

        spark_session_builder = (
            SparkSession.builder.master("local[*]")
            .appName(self.config.app_name)
            .config("spark.sql.extensions", ",".join(self.spark_extensions))
        )

        spark_session_builder = spark_session_builder.config(
            "spark.jars.packages", ",".join(self.spark_packages)
        )

        for key, value in spark_conf.getAll():
            spark_session_builder = spark_session_builder.config(key, value)

        return spark_session_builder.getOrCreate()

    def _get_aws_session(self) -> SparkSession:
        """Create a Spark session for the AWS environment.

        Returns:
            SparkSession: Configured Spark session for AWS environment.
        """
        try:
            from awsglue.context import GlueContext  # type: ignore[import]
        except ImportError:
            logger.error("Could not import GlueContext. Is this running on AWS Glue?")

        spark_conf = SparkConf()
        for key, value in self.spark_config.items():
            spark_conf.set(key, str(value))

        glue_context = GlueContext(SparkContext.getOrCreate(conf=spark_conf))
        return glue_context.spark_session

    @property
    def input(self) -> dict[str, Reader]:
        """Dictionary of input DataReaders used in the application.

        Returns:
            dict[str, DataReader]: dictionary of input DataReaders used in the application, keyed by source name.

        Raises:
            ValueError: If no inputs are configured.
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
            NotImplementedError: If the subclass does not implement this method.
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
