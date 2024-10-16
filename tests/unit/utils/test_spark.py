import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import col, lit, struct

from sparkle.reader.schema_registry import SchemaRegistry
from sparkle.utils.spark import parse_by_avro, table_exists, to_kafka_dataframe
from sparkle.writer.iceberg_writer import IcebergWriter
from tests.conftest import json_to_string


@pytest.mark.parametrize(
    "catalog, database, table",
    [("glue_catalog", "test_db", "test_table")],
)
def test_table_exists(spark_session, teardown_table, catalog, database, table):
    """Test the `table_exists` function for checking table existence in a catalog.

    Args:
        spark_session (SparkSession): The Spark session fixture.
        teardown_table (function): Fixture to clean up by dropping the specified table after the test.
        catalog (str): The catalog where the table is located, provided via parametrization.
        database (str): The database where the table is located, provided via parametrization.
        table (str): The name of the table to test for existence, provided via parametrization.
    """
    data = [{"id": "001", "value": "some_value"}]
    df = spark_session.createDataFrame(data)

    writer = IcebergWriter(
        database_name=database,
        database_path="mock_path",
        table_name=table,
        spark_session=spark_session,
    )
    writer.write(df)

    assert table_exists(database, table, spark_session) is True
    assert table_exists(database, "NON_EXISTENT_TABLE", spark_session) is False


def test_generate_kafka_acceptable_dataframe(user_dataframe: DataFrame, spark_session):
    """Tests the to_kafka_dataframe function to ensure it generates a Kafka-compatible DataFrame.

    This test verifies that the `to_kafka_dataframe` function correctly transforms
    a user DataFrame into the required Kafka format with 'key' and 'value' columns.
    The 'key' column is based on a unique identifier column ('email'), and the 'value'
    column contains a JSON string representation of each row.

    Args:
        user_dataframe (DataFrame): The input DataFrame containing user data with columns such as
            'email', 'name', 'phone', and 'surname'.
        spark_session (SparkSession): The Spark session used for creating DataFrames in the test.
    """
    expected_result = [
        {
            "key": "john@test.com",
            "value": json_to_string(
                {
                    "name": "John",
                    "surname": "Doe",
                    "phone": "12345",
                    "email": "john@test.com",
                },
            ),
        },
        {
            "key": "jane.doe@test.com",
            "value": json_to_string(
                {
                    "name": "Jane",
                    "surname": "Doe",
                    "phone": "12345",
                    "email": "jane.doe@test.com",
                },
            ),
        },
    ]
    expected_df = spark_session.createDataFrame(
        expected_result, schema=["key", "value"]
    )

    actual_df = to_kafka_dataframe("email", user_dataframe)

    assert_df_equality(expected_df, actual_df)


@pytest.fixture
def mock_schema_registry(mocker):
    """Fixture to create a mock schema registry client."""
    mock = mocker.Mock(spec=SchemaRegistry)
    # fmt: off
    mock.cached_schema.return_value = (
        '{"type": "record", "name": "test", "fields":'
        '[{"name": "test", "type": "string"}]}'
    )
    # fmt: on
    return mock


def test_parse_by_avro(spark_session: SparkSession, mock_schema_registry):
    """Test the parse_by_avro function with a mock schema registry and sample DataFrame."""
    schema = mock_schema_registry.cached_schema()

    # Create a DataFrame with a struct matching the Avro schema
    data = [Row(test="value1")]
    df = spark_session.createDataFrame(data).select(struct(col("test")).alias("value"))

    # Convert the DataFrame to Avro format
    avro_df = df.select(to_avro(col("value"), schema).alias("value"))

    # Simulate Kafka message structure
    kafka_data = (
        avro_df.withColumn("key", lit(b"key1").cast("binary"))
        .withColumn("topic", lit("test-topic"))
        .withColumn("partition", lit(0))
        .withColumn("offset", lit(1))
        .withColumn("timestamp", lit(1000))
        .withColumn("timestampType", lit(1))
    )

    # Add magic byte and schema ID to simulate real Kafka Avro messages
    kafka_data = kafka_data.withColumn(
        "value",
        F.concat(F.lit(b"\x00\x00\x00\x00\x01"), col("value")),
    )

    # Create the transformer function using the parse_by_avro function
    transformer = parse_by_avro("test-topic", mock_schema_registry)
    transformed_df = transformer(kafka_data)

    # Check the schema and contents of the transformed DataFrame
    transformed_df.show(truncate=False)
    assert "test" in transformed_df.columns
    assert "__kafka_metadata__" in transformed_df.columns
    assert transformed_df.select(col("test")).collect()[0]["test"] == "value1"
