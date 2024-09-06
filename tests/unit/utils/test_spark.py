import pytest
from sparkle.writer.iceberg_writer import IcebergWriter
from sparkle.utils.spark import table_exists
from sparkle.utils.spark import to_kafka_dataframe
from tests.conftest import json_to_string
from pyspark.sql import DataFrame


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
                    "email": "john@test.com",
                    "name": "John",
                    "phone": "12345",
                    "surname": "Doe",
                },
            ),
        },
        {
            "key": "jane.doe@test.com",
            "value": json_to_string(
                {
                    "email": "jane.doe@test.com",
                    "name": "Jane",
                    "phone": "12345",
                    "surname": "Doe",
                },
            ),
        },
    ]
    expected_df = spark_session.createDataFrame(
        expected_result, schema=["key", "value"]
    )

    df = to_kafka_dataframe("email", user_dataframe)

    assert df.count() == expected_df.count()
    assert expected_df.join(df, ["key"]).count() == expected_df.count()
    assert expected_df.join(df, ["value"]).count() == expected_df.count()
