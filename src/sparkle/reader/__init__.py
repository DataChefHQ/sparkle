from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from sparkle.config import Config


class Reader(ABC):
    """Abstract base class for reading data from a source.

    This class defines the interface for reading data using different configurations and
    Spark sessions. It provides a common structure for specific implementations of data
    readers that can read data from various sources (e.g., databases, files, etc.).
    """

    def __init__(self, config: Config, spark: SparkSession):
        """Initialize the Reader object.

        Args:
            config (Config): Configuration object containing settings for the reader.
            spark (SparkSession): Spark session to be used for reading data.
        """
        self.config = config
        self.spark = spark

    @classmethod
    def with_config(
        cls,
        config: Config,
        spark: SparkSession,
    ):
        """Create a Reader object with a specific configuration.

        This class method allows the creation of a Reader object using the provided
        configuration and Spark session. It is useful for initializing a Reader
        instance with additional parameters passed via kwargs.

        Args:
            config (Config): Configuration object containing settings for the reader.
            spark (SparkSession): Spark session to be used for reading data.
            **kwargs: Additional keyword arguments that may be used by specific
                      implementations of the Reader.

        Returns:
            Reader: An instance of a class that implements the Reader interface.
        """
        return cls(config, spark)

    @abstractmethod
    def read(self) -> DataFrame:
        """Abstract method for reading data from the source.

        This method must be implemented by subclasses to provide the functionality
        for reading data from a specific source. It should return a Spark DataFrame
        containing the data read from the source.

        Returns:
            DataFrame: A Spark DataFrame containing the data read from the source.
        """
        pass
