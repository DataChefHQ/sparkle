import os
import pytest
from pyspark.sql import DataFrame
from sparkle.reader.table_reader import TableReader
from sparkle.config import TableConfig, Config

TEST_DB = "test_db"
TEST_TABLE = "test_table"
CATALOG = "glue_catalog"
WAREHOUSE = "./tmp/warehouse"


@pytest.fixture
def test_db_path(spark_session):
    """Fixture for setting up the test database and performing cleanup.

    This fixture sets up the test database path and drops the test table after the tests.

    Args:
        spark_session (SparkSession): The Spark session fixture.

    Yields:
        str: The path to the test database.
    """
    path = os.path.join(WAREHOUSE, CATALOG, TEST_DB)
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
    yield path
    spark_session.sql(f"DROP TABLE IF EXISTS {CATALOG}.{TEST_DB}.{TEST_TABLE}")


@pytest.fixture
def config():
    """Fixture for creating a configuration object.

    This fixture returns a Config object with necessary attributes set
    for testing the TableReader class.

    Returns:
        Config: A configuration object with test database and table names.
    """
    table_config = TableConfig(
        database=TEST_DB,
        table=TEST_TABLE,
        bucket="test_bucket",
        catalog_name=CATALOG,
        catalog_id=None,
    )
    return Config(
        app_name="test_app",
        app_id="test_id",
        version="1.0",
        database_bucket="test_bucket",
        kafka=None,
        hive_table_input=table_config,
        iceberg_output=None,
    )


def test_table_reader_with_config(spark_session, config):
    """Test the TableReader initialization using a Config object.

    This test verifies that the TableReader initializes correctly using
    the class method with_config and a Config object.

    Args:
        spark_session (SparkSession): The Spark session fixture.
        config (Config): The configuration object.
    """
    reader = TableReader.with_config(spark=spark_session, config=config)

    assert reader.database_name == TEST_DB
    assert reader.table_name == TEST_TABLE
    assert reader.catalog_name == CATALOG
    assert reader.catalog_id is None


def test_qualified_table_name(spark_session):
    """Test the qualified_table_name property of TableReader.

    This test checks that the qualified_table_name property returns the
    correctly formatted string.

    Args:
        spark_session (SparkSession): The Spark session fixture.
    """
    reader = TableReader(
        spark=spark_session,
        database_name=TEST_DB,
        table_name=TEST_TABLE,
        catalog_name=CATALOG,
    )

    assert reader.qualified_table_name == f"{CATALOG}.{TEST_DB}.{TEST_TABLE}"


def test_read_table(spark_session, test_db_path):
    """Test the read method of TableReader.

    This test verifies that the read method correctly reads data from the specified
    table and returns a DataFrame.

    Args:
        spark_session (SparkSession): The Spark session fixture.
        test_db_path (str): Path to the test database.
    """
    # Create a sample table for testing
    spark_session.sql(
        f"CREATE TABLE {CATALOG}.{TEST_DB}.{TEST_TABLE} (id INT, name STRING)"
    )
    spark_session.sql(
        f"INSERT INTO {CATALOG}.{TEST_DB}.{TEST_TABLE} VALUES (1, 'Alice'), (2, 'Bob')"
    )

    reader = TableReader(
        spark=spark_session,
        database_name=TEST_DB,
        table_name=TEST_TABLE,
        catalog_name=CATALOG,
    )

    df = reader.read()

    assert isinstance(df, DataFrame)
    assert df.count() == 2
    assert df.filter(df.name == "Alice").count() == 1
    assert df.filter(df.name == "Bob").count() == 1
