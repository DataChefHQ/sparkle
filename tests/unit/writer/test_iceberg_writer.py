import datetime
import os

import pytest
from pyspark.sql.functions import days

from sparkle.application import PROCESS_TIME_COLUMN
from sparkle.utils.spark import table_exists
from sparkle.writer.iceberg_writer import IcebergWriter

TEST_DB = "default"
TEST_TABLE = "test_table"
WAREHOUSE = "./tmp/test_warehouse"
CATALOG = "glue_catalog"


@pytest.fixture
def test_db_path(spark_session):
    """Fixture for creating the test database path.

    Sets up the path for the test database and performs cleanup by dropping
    the test table after the test is completed.

    Args:
        spark_session (SparkSession): The Spark session fixture.

    Yields:
        str: The path to the test database.
    """
    path = os.path.join(WAREHOUSE, CATALOG, TEST_DB)

    yield path

    # Teardown
    spark_session.sql(f"DROP TABLE IF EXISTS {CATALOG}.{TEST_DB}.{TEST_TABLE}")


@pytest.fixture
def partition_df(spark_session):
    """Fixture for creating a DataFrame with partitioned user data.

    This fixture creates a DataFrame with sample user data, including a timestamp
    column used for partitioning.

    Args:
        spark_session (SparkSession): The Spark session fixture.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with partitioned user data.
    """
    data = [
        {
            "user_id": 1,
            "name": "Bob",
            PROCESS_TIME_COLUMN: datetime.datetime.fromisoformat("2023-11-03").replace(
                tzinfo=datetime.timezone.utc
            ),
        },
        {
            "user_id": 2,
            "name": "Alice",
            PROCESS_TIME_COLUMN: datetime.datetime.fromisoformat("2023-11-02").replace(
                tzinfo=datetime.timezone.utc
            ),
        },
    ]
    return spark_session.createDataFrame(data)


@pytest.fixture
def partition_df_evolved_schema(spark_session):
    """Fixture for creating a DataFrame with an evolved schema.

    This fixture creates a DataFrame that includes an additional field not present
    in the original schema, simulating a schema evolution scenario.

    Args:
        spark_session (SparkSession): The Spark session fixture.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with an evolved schema.
    """
    data = [
        {
            "user_id": 1,
            "name": "Bob",
            "new_field": "new_field_value",
            PROCESS_TIME_COLUMN: datetime.datetime.fromisoformat("2023-11-03").replace(
                tzinfo=datetime.timezone.utc
            ),
        }
    ]
    return spark_session.createDataFrame(data)


def test_writer_should_write_iceberg(user_dataframe, test_db_path, spark_session):
    """Test that the IcebergWriter writes data to the Iceberg table.

    This test verifies that the IcebergWriter correctly writes the provided DataFrame
    to the specified Iceberg table and checks that the table exists afterward.

    Args:
        user_dataframe (pyspark.sql.DataFrame): Fixture providing sample user data.
        test_db_path (str): Path to the test database.
        spark_session (SparkSession): The Spark session fixture.
    """
    writer = IcebergWriter(
        database_name=TEST_DB,
        database_path=test_db_path,
        table_name=TEST_TABLE,
        spark_session=spark_session,
    )

    writer.write(user_dataframe)

    assert table_exists(
        database_name=TEST_DB, table_name=TEST_TABLE, spark=spark_session
    )


def test_write_with_partitions(test_db_path, partition_df, spark_session):
    """Test writing data to Iceberg with partitioning.

    This test checks that data is correctly written to the Iceberg table with partitions
    based on the `PROCESS_TIME_COLUMN`. It verifies the presence of partitioned data files.

    Args:
        test_db_path (str): Path to the test database.
        partition_df (pyspark.sql.DataFrame): DataFrame with partitioned user data.
        spark_session (SparkSession): The Spark session fixture.
    """
    writer = IcebergWriter(
        database_name=TEST_DB,
        database_path=test_db_path,
        table_name=TEST_TABLE,
        spark_session=spark_session,
        partitions=[days(PROCESS_TIME_COLUMN)],
    )

    writer.write(partition_df)

    assert os.path.exists(
        os.path.join(
            test_db_path, TEST_TABLE, "data", f"{PROCESS_TIME_COLUMN}_day=2023-11-02"
        )
    )
    assert os.path.exists(
        os.path.join(
            test_db_path, TEST_TABLE, "data", f"{PROCESS_TIME_COLUMN}_day=2023-11-03"
        )
    )


def test_write_with_partitions_no_partition_column_provided(
    test_db_path, partition_df, spark_session
):
    """Test writing data to Iceberg without specifying partitions.

    This test verifies that data is written to the Iceberg table without any partitions
    when the partition list is explicitly set to empty.

    Args:
        test_db_path (str): Path to the test database.
        partition_df (pyspark.sql.DataFrame): DataFrame with partitioned user data.
        spark_session (SparkSession): The Spark session fixture.
    """
    writer = IcebergWriter(
        database_name=TEST_DB,
        database_path=test_db_path,
        table_name=TEST_TABLE,
        spark_session=spark_session,
        partitions=[],  # Explicitly setting no partitions
    )

    writer.write(partition_df)

    assert os.path.exists(os.path.join(test_db_path, TEST_TABLE, "data"))


def test_write_with_schema_evolution(
    test_db_path, partition_df, partition_df_evolved_schema, spark_session
):
    """Test writing data to Iceberg with schema evolution.

    This test checks that the Iceberg table correctly handles schema evolution by
    adding new fields. It writes initial data and then writes data with an evolved
    schema, verifying that the new field is present in the table schema.

    Args:
        test_db_path (str): Path to the test database.
        partition_df (pyspark.sql.DataFrame): DataFrame with initial schema.
        partition_df_evolved_schema (pyspark.sql.DataFrame): DataFrame with evolved schema.
        spark_session (SparkSession): The Spark session fixture.
    """
    writer = IcebergWriter(
        database_name=TEST_DB,
        database_path=test_db_path,
        table_name=TEST_TABLE,
        spark_session=spark_session,
        partitions=[],
    )

    writer.write(partition_df)
    writer.write(partition_df_evolved_schema)

    final_df = spark_session.table(f"{CATALOG}.{TEST_DB}.{TEST_TABLE}")
    assert "new_field" in final_df.schema.fieldNames()
