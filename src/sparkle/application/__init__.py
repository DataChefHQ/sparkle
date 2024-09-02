import abc
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
from sparkle.config import Config
from sparkle.writer import Writer
from sparkle.data_reader import DataReader


class Sparkle(abc.ABC):
    """Base class for Spark applications.

    This is an abstract base class for defining Spark applications.
    It provides a common interface for processing data and writing the
    results using a set of configurable writers.

    Attributes:
        spark_session (SparkSession): The Spark session used for
          executing Spark jobs.
        config (Config): Configuration object containing
          application-specific settings.
        sources (Dict[str, Source]): Dictionary of data sources used
          in the application, keyed by source name.
        writers (List[Writer]): List of Writer objects used for
          writing the processed data to various destinations.

    """

    def __init__(
        self,
        spark_session: SparkSession,
        config: Config,
        writers: list[Writer],
    ):
        """Sparkle's application initializer.

        Args:
            spark_session (SparkSession): The Spark session used for
              executing Spark jobs.
            config (Config): Configuration object containing
              application-specific settings.
            sources (Dict[str, Source]): Dictionary of data sources
              used in the application, keyed by source name.
            writers (List[Writer]): List of Writer objects used for
              writing the processed data to various destinations.
        """
        self.spark_session = spark_session
        self.config = config
        self.writers = writers

    @property
    def input(self) -> dict[str, DataReader]:
        """Dictionary of input DataReaders used in the application.

        Returns:
            dict[str, DataReader]: Dictionary of input DataReaders
              used in the application, keyed by source name.
        """
        return {
            key: DataReader.with_config(value, config=self.config, spark=self.spark)
            for key, value in self.config.input.items()
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
        """Write output df to application's writer(s).

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
