from typing import Any
from collections.abc import Generator
import pytest
from pyspark.sql import SparkSession, DataFrame
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from sparkle.reader.kafka_reader import KafkaReader, SchemaRegistry

KAFKA_BROKER_URL = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TEST_TOPIC = "test-topic"


@pytest.fixture(scope="session")
def kafka_setup() -> Generator[str, None, None]:
    """Fixture to set up Kafka environment.

    This fixture sets up a Kafka broker, creates a topic, and cleans up after tests.

    Yields:
        str: The Kafka topic name used for testing.
    """
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER_URL})

    admin_client.create_topics(
        [NewTopic(TEST_TOPIC, num_partitions=1, replication_factor=1)]
    )

    yield TEST_TOPIC

    # Cleanup
    admin_client.delete_topics([TEST_TOPIC])


@pytest.fixture
def kafka_producer() -> Producer:
    """Fixture to create a Kafka producer using confluent-kafka.

    Returns:
        Producer: Confluent Kafka producer instance.
    """
    return Producer({"bootstrap.servers": KAFKA_BROKER_URL})


@pytest.fixture
def schema_registry_client() -> SchemaRegistryClient:
    """Fixture to create a Schema Registry client.

    Returns:
        SchemaRegistryClient: A Schema Registry client connected to the Confluent Schema Registry.
    """
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


@pytest.fixture
def avro_serializer(schema_registry_client: SchemaRegistryClient) -> AvroSerializer:
    """Fixture to create an Avro serializer using the Confluent Schema Registry.

    Args:
        schema_registry_client (SchemaRegistryClient): The Schema Registry client fixture.

    Returns:
        AvroSerializer: Serializer for Avro data using the provided schema registry client.
    """
    schema_str = """
    {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }
    """
    # Register the schema with the schema registry
    schema = Schema(schema_str, schema_type="AVRO")
    schema_registry_client.register_schema(f"{TEST_TOPIC}-value", schema)

    return AvroSerializer(schema_registry_client, schema_str)


@pytest.fixture
def kafka_reader(
    spark_session: SparkSession,
    kafka_setup: str,
) -> KafkaReader:
    """Fixture to create a KafkaReader instance with the provided schema registry client.

    Args:
        spark_session (SparkSession): Spark session fixture.
        kafka_setup (str): Kafka topic fixture.

    Returns:
        KafkaReader: Instance of KafkaReader for testing.
    """
    registry = SchemaRegistry(SCHEMA_REGISTRY_URL)

    return KafkaReader(
        spark=spark_session,
        topic=kafka_setup,
        schema_registry=registry,
        use_avro=True,
        schema_version="latest",
        kafka_spark_options={
            "kafka.bootstrap.servers": KAFKA_BROKER_URL,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        },
    )


def produce_avro_message(
    producer: Producer,
    topic: str,
    avro_serializer: AvroSerializer,
    value: dict[str, Any],
) -> None:
    """Produces an Avro encoded message to Kafka.

    Args:
        producer (Producer): Confluent Kafka producer instance.
        topic (str): Kafka topic to write to.
        avro_serializer (AvroSerializer): Avro serializer for encoding the message.
        value (Dict[str, any]): Dictionary representing the Avro value to encode and send.
    """
    string_serializer = StringSerializer("utf_8")
    producer.produce(
        topic=topic,
        key=string_serializer(
            value["name"], SerializationContext(topic, MessageField.KEY)
        ),
        value=avro_serializer(value, SerializationContext(topic, MessageField.VALUE)),
    )
    producer.flush()


def test_kafka_reader_avro(
    spark_session: SparkSession,
    kafka_reader: KafkaReader,
    kafka_producer: Producer,
    avro_serializer: AvroSerializer,
) -> None:
    """Test KafkaReader's ability to read Avro encoded data from Kafka.

    Args:
        spark_session (SparkSession): Spark session fixture.
        kafka_reader (KafkaReader): KafkaReader instance fixture.
        kafka_producer (Producer): Confluent Kafka producer fixture.
        avro_serializer (AvroSerializer): Avro serializer fixture.
    """
    value = {"name": "John Doe", "age": 30}

    # Use writeStream to test the streaming DataFrame
    query = (
        kafka_reader.read()
        .writeStream.format("memory")  # Use an in-memory sink for testing
        .queryName("kafka_test")  # Name the query for querying results
        .outputMode("append")
        .start()
    )

    produce_avro_message(kafka_producer, kafka_reader.topic, avro_serializer, value)

    # Allow the stream to process the data
    query.awaitTermination(20)

    # Query the in-memory table
    result_df: DataFrame = spark_session.sql("SELECT * FROM kafka_test")
    results = result_df.collect()

    assert len(results) > 0, "No data was read from Kafka."
    assert results[0]["name"] == "John Doe"
    assert results[0]["age"] == 30

    query.stop()
