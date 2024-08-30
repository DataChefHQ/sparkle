from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from sparkle.config import Config


class Reader(ABC):
    """Abstract class for reading data from a source."""

    def __init__(self, config: Config, spark: SparkSession):
        """Initialize the Reader object.

        Args:
            config (Config): Configuration for the reader
            spark (SparkSession): Spark session
        """
        self.config = config

    @classmethod
    def with_config(cls, config: Config, spark: SparkSession):
        """Create a Reader object with a configuration.

        Args:
            config (Config): Configuration for the reader
            spark (SparkSession): Spark session

        Returns:
            Reader: Reader object
        """
        return cls(config, spark)

    @abstractmethod
    def read(self):
        """Read data from the source.

        Returns:
            DataFrame: Data read from the source
        """
        pass
