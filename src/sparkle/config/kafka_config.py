from dataclasses import dataclass


@dataclass(frozen=True)
class Credentials:
    """Credentials convention to use for any external services.

    Args:
        username (str): Username.
        password (str): Password.

    Note: the password is stored as a string. Don't log it.
    """

    username: str | None
    password: str | None


@dataclass(frozen=True)
class SchemaRegistryConfig:
    """Schema Registry Configuration."""

    url: str
    credentials: Credentials


@dataclass
class KafkaConfig(frozen=True):
    """Kafka Configuration."""

    bootstrap_servers: str
    credentials: Credentials
    checkpoints_bucket: str
    starting_offset: str = "earliest"
    auth_protocol: str = "SASL_SSL"
    auth_mechanism: str = "PLAIN"
    schema_registry: SchemaRegistryConfig | None
