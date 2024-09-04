import pytest
from sparkle.writer.iceberg_writer import IcebergWriter
from sparkle.utils.spark import table_exists


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
