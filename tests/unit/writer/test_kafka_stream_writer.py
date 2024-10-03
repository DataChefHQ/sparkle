import os
import time
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import floor, rand

from sparkle.writer.kafka_writer import KafkaStreamPublisher

TOPIC = "test-kafka-stream-writer-topic"


@pytest.fixture
def kafka_config(checkpoint_directory) -> dict[str, Any]:
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
        "checkpoint_location": checkpoint_directory + TOPIC,
        "kafka_topic": TOPIC,
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


def test_kafka_stream_publisher_write(
    spark_session: SparkSession,
    rate_stream_dataframe,
    kafka_config: dict[str, Any],
):
    """Test the write method of KafkaStreamPublisher by publishing to Kafka.

    This test validates that the KafkaStreamPublisher can successfully write
    streaming data to a Kafka topic and checks for the presence of commit files
    to confirm that the streaming query has completed.

    Args:
        spark_session (SparkSession): The Spark session used for the test.
        rate_stream_dataframe (DataFrame): The streaming DataFrame to be published.
        kafka_config (dict[str, any]): Kafka configuration options.

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

    # Wait to make sure commit file is created
    time.sleep(5)
    checkpoint_dir = kafka_config["checkpoint_location"]
    commit_file_path = os.path.join(checkpoint_dir, "commits", "0")
    assert os.path.exists(commit_file_path), "Commit file does not exist"
