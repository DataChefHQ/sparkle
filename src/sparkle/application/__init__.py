import abc

from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel

from sparkle.config import Config, ExecutionEnvironment
from sparkle.reader import Reader
from sparkle.utils.logger import logger
from sparkle.writer import Writer

PROCESS_TIME_COLUMN = "process_time"


class Sparkle(abc.ABC):
    """Base class for Spark applications.

    This class provides a foundation for Spark-based data processing applications.
    It handles setting up the Spark session, configuring readers and writers,
    and defining the main process logic through abstract methods that must be implemented
    by subclasses.
    """

    def __init__(
        self,
        config: Config,
        readers: dict[str, type[Reader]],
        writers: list[type[Writer]],
        spark_extensions: list[str] | None = None,
        spark_packages: list[str] | None = None,
        extra_spark_config: dict[str, str] | None = None,
    ):
        """Initializes the Sparkle application with the given configuration.

        Args:
            config (Config): The configuration object containing application-specific settings.
            readers (dict[str, type[Reader]]): A dictionary of readers for input data, keyed by source name.
            writers (list[type[Writer]]): A list of Writer objects used to output processed data.
            spark_extensions (list[str], optional): A list of Spark session extensions to apply.
            spark_packages (list[str], optional): A list of Spark packages to include in the session.
            extra_spark_config (dict[str, str], optional): Additional Spark configurations to
              merge with the default settings.
        """
        self.config = config
        self.readers = readers
        self.execution_env = config.execution_environment
        self.spark_config = config.get_spark_config(
            self.execution_env, extra_spark_config
        )
        self.spark_extensions = config.get_spark_extensions(spark_extensions)
        self.spark_packages = config.get_spark_packages(spark_packages)

        self.spark_session = self.get_spark_session(self.execution_env)
        self.writers: list[Writer] = [
            writer_class.with_config(self.config, self.spark_session)
            for writer_class in writers
        ]

    def get_spark_session(self, env: ExecutionEnvironment) -> SparkSession:
        """Creates and returns a Spark session based on the environment.

        Args:
            env (ExecutionEnvironment): The environment in which the Spark session is created (GENERIC or AWS).

        Returns:
            SparkSession: A Spark session configured for the specified environment.

        Raises:
            NotImplementedError: If an unsupported environment is provided.
        """
        if env == ExecutionEnvironment.GENERIC:
            return self._get_generic_session()
        elif env == ExecutionEnvironment.AWS:
            return self._get_aws_session()
        else:
            raise NotImplementedError(f"Unsupported environment: {env}")

    def _get_generic_session(self) -> SparkSession:
        """Creates a Spark session for generic execution.

        Returns:
            SparkSession: A configured Spark session for the generic environment.
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
        """Creates a Spark session for AWS Glue execution.

        Returns:
            SparkSession: A configured Spark session for the AWS Glue environment.

        Raises:
            ImportError: If the AWS Glue libraries are not available.
        """
        try:
            from awsglue.context import GlueContext  # type: ignore[import]
        except ImportError:
            logger.error("Could not import GlueContext. Is this running on AWS Glue?")
            raise

        spark_conf = SparkConf()
        for key, value in self.spark_config.items():
            spark_conf.set(key, str(value))

        glue_context = GlueContext(SparkContext.getOrCreate(conf=spark_conf))
        return glue_context.spark_session

    @property
    def input(self) -> dict[str, Reader]:
        """Returns the input readers configured for the application.

        Returns:
            dict[str, Reader]: A dictionary mapping input sources to Reader instances.

        Raises:
            ValueError: If no readers are configured for the application.
        """
        if len(self.readers) == 0:
            raise ValueError("No readers configured.")

        return {
            key: value.with_config(self.config, self.spark_session)
            for key, value in self.readers.items()
        }

    @abc.abstractmethod
    def process(self) -> DataFrame:
        """Defines the application's data processing logic.

        This method should be implemented by subclasses to define how the input data
        is processed and transformed into the desired output.

        Returns:
            DataFrame: The resulting DataFrame after the processing logic is applied.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError("process method must be implemented by subclasses")

    def write(self, df: DataFrame) -> None:
        """Writes the output DataFrame to the application's configured writers.

        The DataFrame is persisted in memory to optimize writing operations,
        and once all writers have completed their tasks, the DataFrame is unpersisted.

        Args:
            df (DataFrame): The DataFrame to be written to the output destinations.
        """
        df.persist(StorageLevel.MEMORY_ONLY)

        for writer in self.writers:
            writer.write(df)

        df.unpersist()
