import pytest
from typing import Any
import json
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Create and return a local Spark session configured for use with Iceberg and Kafka.

    This function sets up a local Spark session with specific configurations for Iceberg
    catalog, session extensions, and other relevant settings needed for local testing
    and development. It supports optional custom Ivy settings for managing dependencies.

    Returns:
        SparkSession: A configured Spark session instance for local use.
    """
    _SPARK_EXTENSIONS = [
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    ]

    _SPARK_PACKAGES = [
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.apache.spark:spark-avro_2.12:3.3.0",
    ]
    ivy_settings_path = os.environ.get("IVY_SETTINGS_PATH", None)
    LOCAL_CONFIG = {
        "spark.sql.extensions": ",".join(_SPARK_EXTENSIONS),
        "spark.jars.packages": ",".join(_SPARK_PACKAGES),
        "spark.sql.jsonGenerator.ignoreNullFields": False,
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": "./tmp/test_warehouse",
        "spark.sql.defaultCatalog": "local",
    }

    spark_conf = SparkConf()

    for key, value in LOCAL_CONFIG.items():
        spark_conf.set(key, str(value))

    spark_session = (
        SparkSession.builder.master("local[*]")
        .appName("LocalTestSparkleApp")
        .config(conf=spark_conf)
    )

    if ivy_settings_path:
        spark_session.config("spark.jars.ivySettings", ivy_settings_path)

    return spark_session.getOrCreate()


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
