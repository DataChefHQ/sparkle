from pyspark.sql import SparkSession, DataFrame
from sparkle.config import Config
from sparkle.utils.logger import logger
from sparkle.reader import Reader


class TableReader(Reader):
    """A class for reading tables from a specified catalog using Spark.

    The `TableReader` class provides methods to read data from a table in a specified
    catalog and database using Apache Spark. It supports reading tables with specified
    configurations and provides utility methods to access the fully qualified table name.

    Attributes:
        spark (SparkSession): The Spark session used for reading data.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to read.
        catalog_name (str): The name of the catalog containing the table. Defaults to "glue_catalog".
        catalog_id (Optional[str]): The catalog ID, if applicable. Defaults to None.
    """

    def __init__(
        self,
        spark: SparkSession,
        database_name: str,
        table_name: str,
        catalog_name: str = "glue_catalog",
        catalog_id: str | None = None,
    ):
        """Initializes a TableReader instance.

        Args:
            spark (SparkSession): The Spark session used for reading data.
            database_name (str): The name of the database containing the table.
            table_name (str): The name of the table to read.
            catalog_name (str, optional): The name of the catalog containing the table.
                Defaults to "glue_catalog".
            catalog_id (Optional[str], optional): The catalog ID, if applicable. Defaults to None.
        """
        self.spark = spark
        self.database_name = database_name
        self.table_name = table_name
        self.catalog_name = catalog_name
        self.catalog_id = catalog_id

    @classmethod
    def with_config(
        cls, config: Config, spark: SparkSession, **kwargs
    ) -> "TableReader":
        """Creates a TableReader instance using a configuration object.

        Args:
            spark (SparkSession): The Spark session used for reading data.
            config (Config): The configuration object containing table input configuration.
            **kwargs: Additional keyword arguments passed to the TableReader initializer.

        Returns:
            TableReader: An instance of TableReader configured with the provided settings.

        Raises:
            ValueError: If the input configuration is missing in the provided config.
        """
        if not config.hive_table_input:
            raise ValueError("Hive input configuration is missing.")
        return cls(
            spark=spark,
            database_name=config.hive_table_input.database,
            table_name=config.hive_table_input.table,
            catalog_name=config.hive_table_input.catalog_name,
            catalog_id=config.hive_table_input.catalog_id,
            **kwargs,
        )

    @property
    def qualified_table_name(self) -> str:
        """Gets the fully qualified table name.

        Returns:
            str: The fully qualified table name in the format "catalog_name.database_name.table_name".
        """
        return f"{self.catalog_name}.{self.database_name}.{self.table_name}"

    def read(self) -> DataFrame:
        """Reads the table as a DataFrame.

        This method reads data from the specified table in the configured catalog and database,
        returning it as a Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the data read from the table.
        """
        table_fqdn = self.qualified_table_name
        logger.info(f"Reading dataframe from {table_fqdn}")
        df = self.spark.read.table(table_fqdn)

        logger.info(f"Finished reading dataframe from {table_fqdn}")
        return df
