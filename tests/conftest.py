import pytest
from typing import Any
import json
from pyspark.sql import SparkSession
from sparkle.application.spark import get_local_session


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Fixture for creating a Spark session.

    This fixture creates a Spark session to be used in the tests. It attempts to get
    the active Spark session if available; otherwise, it creates a new one using
    `get_local_session`.

    Returns:
        SparkSession: An active Spark session for use in tests.
    """
    return SparkSession.getActiveSession() or get_local_session()


@pytest.fixture
def user_dataframe(spark_session: SparkSession):
    """Fixture for creating a DataFrame with user data.

    This fixture creates a Spark DataFrame containing sample user data with columns
    for name, surname, phone, and email.

    Args:
        spark_session (SparkSession): The Spark session fixture.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with sample user data.
    """
    data = [
        {
            "name": "John",
            "surname": "Doe",
            "phone": "12345",
            "email": "john@test.com",
        },
        {
            "name": "Jane",
            "surname": "Doe",
            "phone": "12345",
            "email": "jane.doe@test.com",
        },
    ]

    return spark_session.createDataFrame(data)


@pytest.fixture
def teardown_table(spark_session, catalog, database, table):
    """Fixture to drop a specified table after a test.

    This fixture is used to clean up by dropping the specified table after the test
    is completed, ensuring the test environment remains clean.

    Args:
        spark_session (SparkSession): The Spark session fixture.
        catalog (str): The catalog where the table is located.
        database (str): The database where the table is located.
        table (str): The name of the table to drop.

    Yields:
        None
    """
    yield
    spark_session.sql(f"DROP TABLE IF EXISTS {catalog}.{database}.{table}")


def json_to_string(dictionary: dict[str, Any]) -> str:
    """Converts a dictionary to a compact JSON string.

    This function serializes a Python dictionary into a JSON string
    with no indentation, ASCII encoding, and no unnecessary whitespace
    between elements.

    Args:
        dictionary (dict[str, Any]): The dictionary to be converted to a JSON string.

    Returns:
        str: A compact JSON string representation of the input dictionary.
    """
    return json.dumps(
        dictionary,
        indent=0,
        ensure_ascii=True,
        separators=(",", ":"),
    ).replace("\n", "")
