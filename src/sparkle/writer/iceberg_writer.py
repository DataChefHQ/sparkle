from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.readwriter import DataFrameWriterV2
from pyspark.sql.utils import AnalysisException
from sparkle.writer.table_path import TablePath
from sparkle.config import Config
from sparkle.utils.logger import logger
from sparkle.utils.spark import table_exists
from sparkle.writer import Writer


class IcebergWriter(Writer):
    """A writer class for handling data writing operations to Iceberg tables.

    This class provides methods to configure and write data to Iceberg tables,
    including options for schema evolution and partitioning.

    Attributes:
        database_name (str): The name of the database where the table is located.
        database_path (str): The path to the database.
        table_name (str): The name of the table.
        delete_before_write (bool): Flag indicating whether to delete the table before writing. Defaults to False.
        catalog_name (str): The name of the catalog to use. Defaults to "glue_catalog".
        catalog_id (Optional[str]): The ID of the catalog. Defaults to None.
        partitions (Optional[List[Column]]): List of columns used for partitioning. Defaults to None.
        number_of_partitions (int): Number of partitions for the table. Defaults to 1.
        spark (SparkSession): The active Spark session.
    """

    def __init__(
        self,
        database_name: str,
        database_path: str,
        table_name: str,
        delete_before_write: bool = False,
        catalog_name: str = "glue_catalog",
        catalog_id: str | None = None,
        partitions: list[Column] | None = None,
        number_of_partitions: int = 1,
        spark_session: SparkSession | None = SparkSession.getActiveSession(),
    ):
        """Initializes the IcebergWriter with the specified parameters.

        Args:
            database_name (str): The name of the database where the table is located.
            database_path (str): The path to the database.
            table_name (str): The name of the table.
            delete_before_write (bool, optional): Flag to delete the table before writing. Defaults to False.
            catalog_name (str, optional): The name of the catalog to use. Defaults to "glue_catalog".
            catalog_id (Optional[str], optional): The ID of the catalog. Defaults to None.
            partitions (Optional[List[Column]], optional): List of columns used for partitioning. Defaults to None.
            number_of_partitions (int, optional): Number of partitions for the table. Defaults to 1.
            spark_session (Optional[SparkSession], optional): The active Spark session.

        Raises:
            ValueError: If no active Spark session is provided.
        """
        if not spark_session:
            raise ValueError("Spark session is not active")

        self.database_name = database_name
        self.database_path = database_path
        self.table_name = table_name
        self.delete_before_write = delete_before_write
        self.catalog_name = catalog_name
        self.catalog_id = catalog_id
        self.partitions = partitions
        self.number_of_partitions = number_of_partitions
        self.spark = spark_session

    @classmethod
    def with_config(
        cls,
        config: Config,
        spark: SparkSession,
        **kwargs,
    ) -> "IcebergWriter":
        """Creates an IcebergWriter instance using a configuration object.

        Args:
            config (Config): Configuration object containing the output database and database path.
            spark (SparkSession): The active Spark session.
            **kwargs: Additional keyword arguments for initializing the IcebergWriter.

        Returns:
            IcebergWriter: An instance of the IcebergWriter class.

        Raises:
            ValueError: If the Iceberg configuration is not provided in the config object.
        """
        if not config.iceberg_output:
            raise ValueError("Iceberg output configuration is not provided")

        return cls(
            database_name=config.iceberg_output.database_name,
            database_path=config.iceberg_output.database_path,
            table_name=config.iceberg_output.table_name,
            delete_before_write=config.iceberg_output.delete_before_write,
            catalog_name=config.iceberg_output.catalog_name,
            catalog_id=config.iceberg_output.catalog_id,
            partitions=config.iceberg_output.partitions,
            number_of_partitions=config.iceberg_output.number_of_partitions,
            spark_session=spark,
            **kwargs,
        )

    def qualified_table_name(self, table_name: str) -> str:
        """Generates a fully qualified table name using the catalog, database, and table name.

        Args:
            table_name (str): The name of the table.

        Returns:
            str: The fully qualified table name.
        """
        return f"{self.catalog_name}.{self.database_name}.{table_name}"

    def table_path(self, table_name: str) -> TablePath:
        """Generates the table path from the database path and table name.

        Args:
            table_name (str): The name of the table.

        Returns:
            TablePath: The path object representing the table location.
        """
        return TablePath.from_database_table(self.database_path, table_name)

    def write(
        self,
        df: DataFrame,
    ) -> None:
        """Writes a DataFrame to an Iceberg table, optionally handling schema evolution.

        Args:
            df (DataFrame): The DataFrame to write.

        Returns:
            IcebergWriter: The instance of IcebergWriter after the write operation.
        """
        table_fqdn = self.qualified_table_name(self.table_name)

        if self.partitions:
            df = df.repartition(self.number_of_partitions, *self.partitions)
            writer = (
                df.writeTo(table_fqdn).using("iceberg").partitionedBy(*self.partitions)
            )
        else:
            writer = df.writeTo(table_fqdn).using("iceberg")

        if self.delete_before_write:
            writer.createOrReplace()
            return None

        if table_exists(self.database_name, self.table_name, self.spark):
            logger.info(f"Table {table_fqdn} already exists, appending.")
            try:
                writer.append()
            except AnalysisException:
                # TODO AnalysisException may have different causes, if
                # possible, only try schema evolution, if the error is
                # related to that.
                self.append_with_schema_evolution(df, table_fqdn, writer)
                logger.info("Updated table schema successfully.")
        else:
            logger.info(f"Table {table_fqdn} does not exist. Creating...")
            writer.create()

        logger.info(f"Finished writing DataFrame to {table_fqdn}.")

    def append_with_schema_evolution(
        self, df: DataFrame, table_fqdn: str, writer: DataFrameWriterV2
    ) -> None:
        """Appends data to a table with schema evolution, adding new columns if needed.

        Args:
            df (DataFrame): The DataFrame to append.
            table_fqdn (str): The fully qualified table name.
            writer (DataFrameWriterV2): The writer object for the table.

        Returns:
            None
        """
        current_table_df = self.spark.table(table_fqdn)
        current_columns = set(current_table_df.columns)

        new_table_columns = set(df.columns)
        new_columns = new_table_columns.difference(current_columns)
        logger.info("New columns: %s", new_columns)

        for new_column in new_columns:
            new_column_type = df.schema[new_column].dataType.typeName()
            logger.info("Column %s type: %s", new_column, new_column_type)
            query = (
                f"ALTER TABLE {table_fqdn} ADD COLUMN {new_column} {new_column_type}"
            )
            logger.info("Query: %s", query)
            self.spark.sql(query)

        logger.info("Finished updating table schema, appending data.")
        writer.append()
