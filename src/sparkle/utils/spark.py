from sparkle.utils.logger import logger
from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.avro.functions import from_avro
from sparkle.reader.schema_registry import SchemaRegistry
from collections.abc import Callable


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


def parse_by_avro(
    topic: str,
    schema_registry_client: SchemaRegistry,
    options: dict[str, str] = dict(),
) -> Callable[[DataFrame], DataFrame]:
    """Parses Kafka messages in Avro format using a schema from the schema registry.

    This function generates a transformer function that can be applied to a Spark DataFrame
    containing Kafka messages. It uses a schema fetched from the schema registry to parse
    the Avro-encoded message values.

    Args:
        topic (str): The Kafka topic name to fetch the schema for.
        schema_registry_client (SchemaRegistry): Client to interact with the schema registry.
        options (dict[str, str], optional): Additional options for Avro parsing. Defaults to an empty dictionary.

    Returns:
        Callable[[DataFrame], DataFrame]: A transformer function that takes a Spark DataFrame
        and returns a DataFrame with parsed Avro values.

    Example:
        >>> transformer = parse_by_avro("my_topic", schema_registry_client)
        >>> transformed_df = transformer(input_df)

    Raises:
        ValueError: If the schema cannot be fetched from the schema registry.
    """
    schema = schema_registry_client.cached_schema(topic)

    # Skip the first 5 bytes of the value, which is the magic byte and
    # the schema ID. The rest is the Avro value.
    avro_value = F.expr("substring(value, 6, length(value))")

    def transformer(df: DataFrame) -> DataFrame:
        kafka_metadata = "__kafka_metadata__"
        return (
            df.withColumn(
                kafka_metadata,
                F.struct(
                    df.key,
                    df.topic,
                    df.partition,
                    df.offset,
                    df.timestamp,
                    df.timestampType,
                ),
            )
            .withColumn("value", from_avro(avro_value, schema, options=options))
            .select("value.*", kafka_metadata)
        )

    return transformer
