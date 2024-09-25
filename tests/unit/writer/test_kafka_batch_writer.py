from typing import Any

import pytest
from chispa.dataframe_comparer import assert_df_equality
from confluent_kafka.admin import AdminClient, NewTopic

from sparkle.config.kafka_config import SchemaFormat
from sparkle.reader.kafka_reader import KafkaReader
from sparkle.reader.schema_registry import SchemaRegistry
from sparkle.writer.kafka_writer import KafkaBatchPublisher

TOPIC = "test-kafka-batch-writer-topic"
BROKER_URL = "localhost:9092"


@pytest.fixture
def kafka_config(user_dataframe, checkpoint_directory) -> dict[str, Any]:
    """Fixture that provides Kafka configuration options for testing.

    Returns:
        dict[str, any]: A dictionary containing Kafka configuration options,
        including Kafka bootstrap servers, security protocol, Kafka topic,
        and unique identifier column name.
    """
    return {
        "kafka_options": {
            "kafka.bootstrap.servers": BROKER_URL,
            "kafka.security.protocol": "PLAINTEXT",
        },
        "checkpoint_location": checkpoint_directory + TOPIC,
        "kafka_topic": TOPIC,
        "unique_identifier_column_name": user_dataframe.columns[0],
    }


@pytest.fixture
def kafka_setup():
    """Create a Kafka topic and deletes it after the test."""
    kafka_client = AdminClient({"bootstrap.servers": BROKER_URL})
    kafka_client.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    yield
    kafka_client.delete_topics([TOPIC])


def test_kafka_batch_publisher_write(
    user_dataframe,
    kafka_config,
    spark_session,
    mocker,
    kafka_setup,
):
    """Test the write method of KafkaBatchPublisher by publishing to Kafka."""
    publisher = KafkaBatchPublisher(
        kafka_options=kafka_config["kafka_options"],
        kafka_topic=kafka_config["kafka_topic"],
        unique_identifier_column_name=kafka_config["unique_identifier_column_name"],
        spark=spark_session,
    )
    reader = KafkaReader(
        spark=spark_session,
        topic=kafka_config["kafka_topic"],
        schema_registry=mocker.Mock(spec=SchemaRegistry),
        format_=SchemaFormat.raw,
        schema_version="latest",
        kafka_spark_options={
            "kafka.bootstrap.servers": kafka_config["kafka_options"]["kafka.bootstrap.servers"],
            "startingOffsets": "earliest",
            "enable.auto.commit": True,
        },
    )

    publisher.write(user_dataframe)
    query = (
        reader.read()
        .writeStream.format("memory")
        .queryName("batch_data")
        .outputMode("append")
        .option("checkpointLocation", kafka_config["checkpoint_location"])
        .trigger(once=True)
        .start()
    )
    query.awaitTermination(timeout=10)

    actual_df = spark_session.sql("""
    SELECT
        parsed_json.*
    FROM (
        SELECT
            from_json(
                cast(value as string),
                'name STRING, surname STRING, phone STRING, email STRING'
            ) as parsed_json
        FROM batch_data
    )
    """)

    expected_df = user_dataframe
    assert_df_equality(expected_df, actual_df, ignore_row_order=True)
