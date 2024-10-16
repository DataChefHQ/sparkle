from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from sparkle.config import Config


class Writer(ABC):
    """Abstract base class for writing DataFrames.

    Args:
        spark (SparkSession): The Spark session to be used for writing data.
        **kwargs (Any): Additional keyword arguments for specific writer implementations.
    """

    def __init__(self, spark: SparkSession, **kwargs: Any):
        """Initialize the Writer object.

        Args:
            spark (SparkSession): The Spark session to be used for writing data.
            **kwargs (Any): Additional keyword arguments for specific writer implementations.
        """
        self.spark = spark

    @classmethod
    def with_config(
        cls, config: Config, spark: SparkSession, **kwargs: Any
    ) -> "Writer":
        """Create a Writer object with a configuration.

        This class method initializes a Writer instance using the provided configuration
        and Spark session. Specific implementations of the Writer class should override
        this method to handle additional configuration settings as needed.

        Args:
            config (Config): Configuration object containing settings for the writer.
            spark (SparkSession): The Spark session to be used for writing data.
            **kwargs (Any): Additional keyword arguments that may be used by specific
                            implementations of the Writer.

        Returns:
            Writer: An instance of a class that implements the Writer interface.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("with_config method must be implemented")

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write data from a DataFrame.

        This abstract method must be implemented by subclasses to provide the functionality
        for writing data from a Spark DataFrame to a specified target.

        Args:
            df (DataFrame): The DataFrame to be written.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("write method must be implemented")
