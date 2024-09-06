from sparkle.utils.logger import logger
from pyspark.sql import SparkSession, functions as F, DataFrame


def table_exists(
    database_name: str,
    table_name: str,
    spark: SparkSession | None = SparkSession.getActiveSession(),
    catalog_name: str = "glue_catalog",
) -> bool:
    """Checks if a table exists in the specified catalog and database.

    Args:
        database_name (str): The name of the database where the table is located.
        table_name (str): The name of the table to check for existence.
        spark (SparkSession | None, optional): The current active Spark session.
            Defaults to the active Spark session if not provided.
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


def to_kafka_dataframe(unique_identifier_column_name: str, df: DataFrame) -> DataFrame:
    """Generates a DataFrame with Kafka-compatible columns 'key' and 'value'.

    This function transforms the input DataFrame to have a structure required
    for the Spark Kafka writer API:
    - 'key': The primary key of the table, used as the Kafka key.
    - 'value': A JSON representation of the table row.

    Args:
        unique_identifier_column_name (str): The name of the column to be used as the Kafka key.
        df (DataFrame): The input DataFrame to be transformed.

    Returns:
        DataFrame: A transformed DataFrame with 'key' and 'value' columns suitable for Kafka streaming.
    """
    return df.select(
        [
            # Use PK of table as Kafka key
            F.col(unique_identifier_column_name).alias("key"),
            F.to_json(F.struct([df[f] for f in df.columns])).alias("value"),
        ]
    )
