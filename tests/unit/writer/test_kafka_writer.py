import pytest
from typing import Any
import os
import shutil
from sparkle.writer.kafka_writer import KafkaStreamPublisher
from pyspark.sql.functions import floor, rand
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


@pytest.fixture
def kafka_config() -> dict[str, Any]:
    """Fixture that provides Kafka configuration options for testing.

    Returns:
        dict[str, any]: A dictionary containing Kafka configuration options,
        including Kafka bootstrap servers, security protocol, checkpoint location,
        Kafka topic, output mode, unique identifier column name, and trigger options.
    """
    return {
        "kafka_options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "kafka.security.protocol": "PLAINTEXT",
        },
        "checkpoint_location": "./tmp/checkpoint",
        "kafka_topic": "test_topic",
        "output_mode": "append",
        "unique_identifier_column_name": "id",
        "trigger_once": True,
    }


@pytest.fixture
def rate_stream_dataframe(spark_session) -> DataFrame:
    """Fixture that generates a rate stream DataFrame with an additional random id column.

    Args:
        spark_session (SparkSession): The Spark session used to create the DataFrame.

    Returns:
        DataFrame: A streaming DataFrame with a random 'id' column consisting of
        random integers between 1 and 1000, cast to string type.
    """
    rate_df = spark_session.readStream.format("rate").option("rowsPerSecond", 2).load()

    # Add a random id column with random integers between 1 and 1000
    rate_df = rate_df.withColumn("id", floor(rand() * 1000 + 1).cast("string"))

    return rate_df


@pytest.fixture
def cleanup_checkpoint_directory(kafka_config):
    """Fixture that validates and removes the checkpoint directory after tests.

    Args:
        kafka_config (dict[str, any]): The Kafka configuration dictionary.

    Yields:
        None: This fixture ensures that the checkpoint directory specified in the
        Kafka configuration is removed after test execution if it exists.
    """
    checkpoint_dir = kafka_config["checkpoint_location"]

    yield

    # Remove the checkpoint directory if it exists
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
        print(f"Checkpoint directory {checkpoint_dir} has been removed.")


def test_kafka_stream_publisher_write(
    spark_session: SparkSession,
    rate_stream_dataframe,
    kafka_config: dict[str, Any],
    cleanup_checkpoint_directory,
):
    """Test the write method of KafkaStreamPublisher by publishing to Kafka.

    This test validates that the KafkaStreamPublisher can successfully write
    streaming data to a Kafka topic and checks for the presence of commit files
    to confirm that the streaming query has completed.

    Args:
        spark_session (SparkSession): The Spark session used for the test.
        rate_stream_dataframe (DataFrame): The streaming DataFrame to be published.
        kafka_config (dict[str, any]): Kafka configuration options.
        cleanup_checkpoint_directory: Fixture to clean up the checkpoint directory.

    Raises:
        AssertionError: If the commit file does not exist after the stream terminates.
    """
    publisher = KafkaStreamPublisher(
        kafka_options=kafka_config["kafka_options"],
        checkpoint_location=kafka_config["checkpoint_location"],
        kafka_topic=kafka_config["kafka_topic"],
        output_mode=kafka_config["output_mode"],
        unique_identifier_column_name=kafka_config["unique_identifier_column_name"],
        spark=spark_session,
        trigger_once=kafka_config["trigger_once"],
    )

    try:
        publisher.write(rate_stream_dataframe)
        # Since trigger_once is True, wait for the stream to finish
        spark_session.streams.awaitAnyTermination(timeout=60)
    except Exception as e:
        pytest.fail(f"KafkaStreamPublisher write failed with exception: {e}")

    checkpoint_dir = kafka_config["checkpoint_location"]
    commit_file_path = os.path.join(checkpoint_dir, "commits", "0")
    assert os.path.exists(commit_file_path), "Commit file does not exist"
