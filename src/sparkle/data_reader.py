"""This module contains the `DataReader` protocol and its implementations."""

from typing import Protocol
from pyspark.sql import DataFrame, SparkSession

from .models import InputField


class DataReader(Protocol):
    """A protocol to define the interface of a data reader."""

    def read(
        self, inputs: list[InputField], spark_session: SparkSession
    ) -> dict[str, DataFrame]:
        """Read data from the source and return the dataframes."""
        ...


class IcebergReader(DataReader):
    """A data reader to read data from Iceberg databases."""

    def __init__(self, database_name: str) -> None:
        """Initialize the IcebergReader with the given database name."""
        super().__init__()
        self.database_name = database_name

    def read(
        self, inputs: list[InputField], spark_session: SparkSession
    ) -> dict[str, DataFrame]:
        """Read data from the Iceberg database and return the dataframes."""
        raise NotImplementedError


class SqlReader(DataReader):
    """A data reader to read data from SQL databases."""

    def __init__(
        self,
        username: str,
        password: str,
        server_name: str = "localhost",
    ) -> None:
        """Initialize the SqlReader with the given credentials."""
        super().__init__()
        self.username = username
        self.password = password
        self.server_name = server_name

    def read(
        self, inputs: list[InputField], spark_session: SparkSession
    ) -> dict[str, DataFrame]:
        """Read data from the SQL database and return the dataframes."""
        raise NotImplementedError


class KafkaReader(DataReader):
    """A data reader to read data from Kafka."""

    def __init__(self, server: str) -> None:
        """Initialize the KafkaReader with the given server."""
        super().__init__()
        self.server = server

    def read(
        self, inputs: list[InputField], spark_session: SparkSession
    ) -> dict[str, DataFrame]:
        """Read data from the Kafka server and return the dataframes."""
        for input in inputs:
            if _ := input.options.get("topic"):
                raise NotImplementedError
            else:
                raise ValueError(
                    "Option `topic` must be provided in the `InputField` with KafkaReader types."
                )

        raise NotImplementedError


class MultiDataReader(DataReader):
    """A data reader to read data from multiple data readers."""

    def __init__(self, *readers: DataReader) -> None:
        """Initialize the MultiDataReader with the given data readers."""
        super().__init__()
        self._readers = list(readers)

    def read(
        self, inputs: list[InputField], spark_session: SparkSession
    ) -> dict[str, DataFrame]:
        """Read data from the multiple data readers and return the dataframes."""
        dataframes = {}

        for reader in self._readers:
            reader_inputs = [
                input for input in inputs if input.type == reader.__class__
            ]
            dfs = reader.read(reader_inputs, spark_session)
            dataframes.update(dfs)

        return dataframes
