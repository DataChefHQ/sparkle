import requests  # type: ignore
from requests.auth import HTTPBasicAuth  # type: ignore
from sparkle.config import Config
from enum import Enum


class SubjectNameStrategy(Enum):
    """Enumeration of subject name strategies for schema registry."""

    TOPIC_NAME = "topic_name"
    RECORD_NAME = "record_name"
    TOPIC_RECORD_NAME = "topic_record_name"


class SchemaRegistry:
    """A client for interacting with a schema registry.

    This class provides methods to fetch, cache, and register schemas in a schema
    registry using HTTP requests. It supports only the `TOPIC_NAME` strategy for
    subject naming.

    Attributes:
        schema_registry_url (str): URL of the schema registry.
        username (str | None): Username for authentication.
        password (str | None): Password for authentication.
        __cache (dict[str, str]): Internal cache for storing fetched schemas.
    """

    def __init__(
        self,
        schema_registry_url: str,
        username: str | None = None,
        password: str | None = None,
        subject_name_strategy: SubjectNameStrategy = SubjectNameStrategy.TOPIC_NAME,
    ):
        """Initializes a SchemaRegistry client.

        Args:
            schema_registry_url (str): URL of the schema registry.
            username (str | None, optional): Username for authentication. Defaults to None.
            password (str | None, optional): Password for authentication. Defaults to None.
            subject_name_strategy (SubjectNameStrategy, optional): Strategy for naming subjects.
                Only `TOPIC_NAME` is supported. Defaults to `SubjectNameStrategy.TOPIC_NAME`.

        Raises:
            NotImplementedError: If a strategy other than `TOPIC_NAME` is used.
        """
        if subject_name_strategy != SubjectNameStrategy.TOPIC_NAME:
            raise NotImplementedError(
                "Only SubjectNameStrategy.TOPIC_NAME is supported at the moment."
            )

        self.schema_registry_url = schema_registry_url
        self.username = username
        self.password = password
        self.__cache: dict[str, str] = {}

    def _subject_name(self, topic: str) -> str:
        """Generates the subject name based on the topic.

        Args:
            topic (str): The Kafka topic name.

        Returns:
            str: The subject name formatted as '<topic>-value'.
        """
        return f"{topic}-value"

    @classmethod
    def with_config(cls, config: Config) -> "SchemaRegistry":
        """Creates a SchemaRegistry instance from a Config object.

        This method initializes a SchemaRegistry instance using configuration
        details provided in the `Config` object.

        Args:
            config (Config): Configuration object containing settings for the schema registry.

        Returns:
            SchemaRegistry: An instance of SchemaRegistry configured with the provided settings.

        Raises:
            ValueError: If the necessary Kafka input or schema registry settings are not configured.
        """
        if not config.kafka_input:
            raise ValueError("Kafka input is not configured.")

        if not config.kafka_input.kafka_config:
            raise ValueError("Kafka config is not configured.")

        if not config.kafka_input.kafka_config.schema_registry:
            raise ValueError("Schema registry is not configured.")

        if not config.kafka_input.kafka_config.schema_registry.credentials:
            raise ValueError("Schema registry credentials are not configured.")

        return cls(
            schema_registry_url=config.kafka_input.kafka_config.schema_registry.url,
            username=config.kafka_input.kafka_config.schema_registry.credentials.username,
            password=config.kafka_input.kafka_config.schema_registry.credentials.password,
        )

    @property
    def _auth(self) -> HTTPBasicAuth | None:
        """Generates HTTP basic authentication credentials.

        Returns:
            HTTPBasicAuth | None: Authentication object if username and password are set, otherwise None.
        """
        if self.username and self.password:
            return HTTPBasicAuth(self.username, self.password)
        return None

    def fetch_schema(self, topic: str, version: str = "latest") -> str:
        """Fetches the schema from the schema registry for a given topic and version.

        This method retrieves the schema for the specified topic and version from the schema registry.

        Args:
            topic (str): The Kafka topic name.
            version (str, optional): The schema version to fetch. Defaults to "latest".

        Returns:
            str: The fetched schema as a string.

        Raises:
            ValueError: If the fetched schema is not a string.
            requests.HTTPError: If the request to the schema registry fails.
        """
        subject = self._subject_name(topic)
        url = f"{self.schema_registry_url}/subjects/{subject}/versions/{version}"

        response = requests.get(url, auth=self._auth)
        response.raise_for_status()

        schema = response.json()["schema"]

        if not isinstance(schema, str):
            raise ValueError(f"Schema is not a string: {schema}")

        return schema

    def cached_schema(self, topic: str, version: str = "latest") -> str:
        """Retrieves the schema from the cache or fetches it if not cached.

        Args:
            topic (str): The Kafka topic name.
            version (str, optional): The schema version to fetch. Defaults to "latest".

        Returns:
            str: The cached or fetched schema as a string.
        """
        if topic not in self.__cache:
            self.__cache[topic] = self.fetch_schema(topic, version)

        return self.__cache[topic]

    def put_schema(self, topic: str, schema: str) -> int:
        """Registers a schema with the schema registry for a given topic.

        Args:
            topic (str): The Kafka topic name.
            schema (str): The schema to register as a string.

        Returns:
            int: The ID of the registered schema.

        Raises:
            ValueError: If the schema ID returned is not a valid integer.
            requests.HTTPError: If the request to the schema registry fails.
        """
        subject = self._subject_name(topic)
        url = f"{self.schema_registry_url}/subjects/{subject}/versions"

        response = requests.post(url, auth=self._auth, json={"schema": schema})
        response.raise_for_status()

        schema_id = response.json()["id"]
        if not isinstance(schema_id, int):
            raise ValueError(f"Schema ID is not of valid type: {schema_id}")

        return schema_id
