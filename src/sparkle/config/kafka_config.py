from enum import Enum
from dataclasses import dataclass


class SchemaFormat(Enum):
    """Enumeration for different schema types.

    Attributes:
        RAW (str): Raw schema type.
        AVRO (str): Avro schema type.
    """

    raw = "raw"
    avro = "avro"


@dataclass(frozen=True)
class Credentials:
    """Credentials for external services.

    Attributes:
        username (str): Username for the service.
        password (str): Password for the service.
    """

    username: str
    password: str


@dataclass(frozen=True)
class SchemaRegistryConfig:
    """Configuration for the Schema Registry.

    Attributes:
        url (str): URL of the Schema Registry.
        credentials (Credentials): Credentials for accessing the Schema Registry.
        schema_format (SchemaFormat): Format of schema to use with the Schema Registry.
    """

    url: str
    credentials: Credentials
    schema_format: SchemaFormat


@dataclass(frozen=True)
class KafkaConfig:
    """Configuration for Kafka.

    Attributes:
        bootstrap_servers (str): Kafka bootstrap servers.
        credentials (Credentials): Credentials for Kafka.
        auth_protocol (str): Authentication protocol for Kafka. Defaults to 'SASL_SSL'.
        auth_mechanism (str): Authentication mechanism for Kafka. Defaults to 'PLAIN'.
        schema_registry (Optional[SchemaRegistryConfig]): Configuration for Schema Registry.
    """

    bootstrap_servers: str
    credentials: Credentials
    auth_protocol: str = "SASL_SSL"
    auth_mechanism: str = "PLAIN"
    schema_registry: SchemaRegistryConfig | None = None

    @property
    def _jaas_config(self) -> str:
        """Generates the JAAS configuration string for Kafka authentication.

        Returns:
            str: JAAS configuration string required for Kafka authentication.

        Raises:
            ValueError: If Kafka credentials are missing.
        """
        if not self.credentials.username or not self.credentials.password:
            raise ValueError("Username and password must be provided for Kafka")

        return (
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.credentials.username}" '
            f'password="{self.credentials.password}";'
        )

    @property
    def spark_kafka_config(self) -> dict[str, str]:
        """Generates Spark configuration settings for Kafka.

        Returns:
            dict[str, str]: Dictionary of Kafka configurations for Spark.
        """
        config = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "kafka.security.protocol": self.auth_protocol,
            "kafka.sasl.mechanism": self.auth_mechanism,
            "kafka.sasl.jaas.config": self._jaas_config,
        }

        if self.schema_registry:
            config.update(
                {
                    "schema.registry.url": self.schema_registry.url,
                    "basic.auth.credentials.source": "USER_INFO",
                    "basic.auth.user.info": (
                        f"{self.schema_registry.credentials.username}:"
                        f"{self.schema_registry.credentials.password}"
                    ),
                }
            )

        return config


class KafkaReaderConfig:
    """Configuration for reading from Kafka.

    Attributes:
        kafka_config (KafkaConfig): Configuration object for Kafka.
        kafka_topic (str): The Kafka topic to read from.
        starting_offsets (str): Starting offsets for reading. Defaults to 'earliest'.
    """

    kafka_config: KafkaConfig
    kafka_topic: str
    starting_offsets: str

    def __init__(
        self,
        kafka_config: KafkaConfig,
        kafka_topic: str,
        starting_offsets: str = "earliest",
    ):
        """Initializes KafkaReaderConfig with Kafka configuration, topic, and starting offsets.

        Args:
            kafka_config (KafkaConfig): Kafka configuration.
            kafka_topic (str): The Kafka topic to read from.
            starting_offsets (str): The starting offsets for reading. Defaults to 'earliest'.
        """
        self.kafka_config = kafka_config
        self.kafka_topic = kafka_topic
        self.starting_offsets = starting_offsets

    @property
    def spark_kafka_config(self) -> dict[str, str]:
        """Generates Spark configuration settings for Kafka, including the starting offsets.

        Returns:
            dict[str, str]: Dictionary of Kafka configurations for Spark, including the starting offsets.
        """
        spark_config = self.kafka_config.spark_kafka_config
        spark_config.update({"startingOffsets": self.starting_offsets})

        return spark_config


class KafkaWriterConfig:
    """Configuration for writing to Kafka.

    Attributes:
        kafka_config (KafkaConfig): Configuration object for Kafka.
        kafka_topic (str): The Kafka topic to write to.
        trigger_once (bool): Flag indicating whether the writer should trigger only once.
        output_mode (str): The output mode for writing data to Kafka. Defaults to 'append'.
        unique_identifier_column_name (str): Column name to be used as Kafka key.
    """

    kafka_config: KafkaConfig
    kafka_topic: str
    trigger_once: bool
    output_mode: str
    unique_identifier_column_name: str

    def __init__(
        self,
        kafka_config: KafkaConfig,
        kafka_topic: str,
        unique_identifier_column_name: str,
        trigger_once: bool = True,
        output_mode: str = "append",
    ):
        """Initializes KafkaWriterConfig with Kafka configuration, topic, and trigger setting.

        Args:
            kafka_config (KafkaConfig): Kafka configuration.
            kafka_topic (str): The Kafka topic to write to.
            unique_identifier_column_name (str): Column name to be used as Kafka key.
            trigger_once (bool, optional): Flag indicating whether the writer should trigger only once.
                Defaults to True.
            output_mode (str, optional): The output mode for writing data to Kafka.
                Defaults to 'append'.
        """
        self.kafka_config = kafka_config
        self.kafka_topic = kafka_topic
        self.trigger_once = trigger_once
        self.output_mode = output_mode
        self.unique_identifier_column_name = unique_identifier_column_name
