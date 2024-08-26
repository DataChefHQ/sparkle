"""A module to define data writers."""

from typing import Protocol
from pyspark.sql import DataFrame, SparkSession


class DataWriter(Protocol):
    """A protocol to define the interface of a data writer."""

    def write(self, df: DataFrame, spark_session: SparkSession) -> None:
        """Write data to the destination."""
        ...


class KafkaWriter(DataWriter):
    """A data writer to write data to Kafka."""

    def __init__(self, server: str) -> None:
        """Initialize the KafkaWriter with the given server."""
        super().__init__()
        self._server = server

    def write(self, df: DataFrame, spark_session: SparkSession) -> None:
        """Write data to the Kafka server."""
        df.write.format("kafka").save()


class IcebergWriter(DataWriter):
    """A data writer to write data to Iceberg databases."""

    def __init__(self, database_name: str) -> None:
        """Initialize the IcebergWriter with the given database name."""
        super().__init__()
        self._database_name = database_name

    def write(self, df: DataFrame, spark_session: SparkSession) -> None:
        """Write data to the Iceberg database."""
        df.write.format("iceberg").save()


class MultiDataWriter(DataWriter):
    """A data writer to write data to multiple data writers."""

    def __init__(self, *writers: DataWriter) -> None:
        """Initialize the MultiDataWriter with the given data writers."""
        super().__init__()
        self._writers = list(writers)

    def write(self, df: DataFrame, spark_session: SparkSession) -> None:
        """Write data to the destinations."""
        for writer in self._writers:
            writer.write(df, spark_session)
