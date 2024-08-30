from sparkle.utils.logger import logger
from pyspark.sql import SparkSession


def table_exists(
    database_name: str,
    table_name: str,
    spark: SparkSession | None = SparkSession.getActiveSession(),
    catalog_name: str = "glue_catalog",
) -> bool:
    """
    Checks if a table exists in the specified catalog and database.

    Args:
        database_name (str): The name of the database where the table is located.
        table_name (str): The name of the table to check for existence.
        spark (SparkSession | None, optional): The current active Spark session. Defaults to the active Spark session if not provided.
        catalog_name (str, optional): The name of the catalog to search in. Defaults to "glue_catalog".

    Returns:
        bool: True if the table exists in the specified catalog and database, False otherwise.

    Raises:
        ValueError: If the Spark session is not active.

    """
    if not spark:
        raise ValueError("Spark session is not active")

    try:
        return (
            spark.sql(
                f"show tables in {catalog_name}.{database_name} like '{table_name}'"
            ).count()
            == 1
        )
    except Exception as e:
        logger.warning(e)
        return False
