from abc import ABC, abstractmethod
from typing import Any
from pyspark.sql import SparkSession, DataFrame
from sparkle.config import Config


class Writer(ABC):
    """Abstract base class for writing DataFrames.

    Args:
        spark (SparkSession): The Spark session to be used for writing data.
    """

    def __init__(self, spark: SparkSession, **kwargs: Any):
        """Initialize the Writer object.

        Args:
            config (Config): Configuration for the writer
            spark (SparkSession): Spark session
        """
        self.spark = spark

    @classmethod
    def with_config(
        cls, config: Config, spark: SparkSession, **kwargs: Any
    ) -> "Writer":
        """Create a Writer object with a configuration.

        Args:
            config (Config): Configuration for the writer
            spark (SparkSession): Spark session

        Returns:
            Writer: Writer object
        """
        raise NotImplementedError("with_config method must be implemented")

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Writes data from a DataFrame.

        Args:
            df (DataFrame): The DataFrame to be written.
        """
        raise NotImplementedError("write method must be implemented")
