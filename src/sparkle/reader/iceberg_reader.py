from pyspark.sql import SparkSession, DataFrame

from sparkle.config import Config
from sparkle.reader import Reader
from sparkle.utils.logger import logger


class TableReader(Reader):
    def __init__(
        self,
        spark: SparkSession,
        database_name: str,
        table_name: str,
        catalog_name: str = "glue_catalog",
        catalog_id: str | None = None,
    ):
        """Read Tables."""
        self.spark = spark
        self.database_name = database_name
        self.catalog_name = catalog_name
        self.catalog_id = catalog_id
        self.table_name = table_name

    @classmethod
    def with_config(
        cls, spark: SparkSession, config: Config, **kwargs
    ) -> "TableReader":
        if not config.iceberg_config:
            raise ValueError(
                "Iceberg configuration is not provided in the config object."
            )

        return cls(
            spark=spark,
            database_name=config.iceberg_config.database_name,
            table_name=config.iceberg_config.table_name,
            **kwargs,
        )

    def qualified_table_name(self) -> str:
        return f"{self.catalog_name}.{self.database_name}.{self.table_name}"

    def read(self) -> DataFrame:
        table_fqdn = self.qualified_table_name()

        logger.info(f"Reading dataframe from {table_fqdn}")
        df = self.spark.read.table(table_fqdn)

        logger.info(f"finished reading dataframe from {table_fqdn}")
        return df
