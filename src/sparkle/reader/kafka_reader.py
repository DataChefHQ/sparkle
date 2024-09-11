from pyspark.sql import SparkSession, DataFrame
from sparkle.config import Config
from sparkle.reader.schema_registry import SchemaRegistry
from sparkle.utils.spark import parse_by_avro


class KafkaReader:
    """KafkaReader is a reader for streaming data from Kafka using Spark.

    This class allows you to read data from a specified Kafka topic, with support
    for Avro format parsing using a schema registry.

    Attributes:
        topic (str): Kafka topic to read from.
        schema_registry (SchemaRegistry): Schema registry client for fetching Avro schemas.
        schema_version (str): Version of the schema to use for Avro parsing.
        use_avro (bool): Flag indicating whether to parse data using Avro format.
        kafka_options (dict): Dictionary containing Kafka configuration options for Spark.
    """

    def __init__(
        self,
        spark: SparkSession,
        topic: str,
        schema_registry: SchemaRegistry,
        use_avro: bool = True,
        schema_version: str = "latest",
        kafka_spark_options: dict[str, str] = {},
    ):
        """Initializes KafkaReader with configuration, Spark session, topic, and schema registry.

        Args:
            config (Config): Configuration object containing Kafka settings.
            spark (SparkSession): Spark session to be used for reading data.
            topic (str): Kafka topic to read from.
            schema_registry (SchemaRegistry): Schema registry client for fetching Avro schemas.
            use_avro (bool, optional): Whether to use Avro format for parsing data. Defaults to True.
            schema_version (str, optional): Schema version to use for reading data. Defaults to "latest".
            kafka_spark_options (dict[str, str], optional): Dictionary containing Kafka configuration options
                for Spark.

        Raises:
            ValueError: If Kafka input configuration or Kafka configuration is missing in the provided config.
        """
        self.spark = spark
        self.topic = topic
        self.schema_registry = schema_registry
        self.schema_version = schema_version
        self.use_avro = use_avro
        self.kafka_options = kafka_spark_options

    @classmethod
    def with_config(cls, config: Config, spark: SparkSession, **kwargs):
        """Creates a KafkaReader instance with specific configuration.

        Args:
            config (Config): Configuration object containing Kafka settings.
            spark (SparkSession): Spark session to be used for reading data.
            **kwargs: Additional keyword arguments, such as topic, schema registry, and schema version.

        Returns:
            KafkaReader: An instance of KafkaReader configured with the provided settings.

        Raises:
            ValueError: If Kafka input configuration is missing in the provided config.
        """
        if not config.kafka_input:
            raise ValueError("Kafka input configuration is missing.")

        schema_registry = SchemaRegistry.with_config(config)

        return cls(
            spark=spark,
            topic=config.kafka_input.kafka_topic,
            schema_registry=schema_registry,
        )

    def read_raw(self) -> DataFrame:
        """Reads raw data from the Kafka topic as a Spark DataFrame.

        This method connects to the Kafka topic and reads data as a raw Spark
        DataFrame without applying any format-specific parsing.

        Returns:
            DataFrame: A Spark DataFrame containing the raw data from the Kafka topic.
        """
        df = (
            self.spark.readStream.format("kafka")
            .option("subscribe", self.topic)
            .options(**self.kafka_options)
            .load()
        )
        return df

    def read(self) -> DataFrame:
        """Reads data from the Kafka topic, optionally parsing it using Avro.

        Returns:
            DataFrame: A Spark DataFrame containing the data read from the Kafka topic.
        """
        if self.use_avro:
            return self.read_avro()
        return self.read_raw()

    def read_avro(self) -> DataFrame:
        """Reads Avro data from the Kafka topic and parses it using the schema registry.

        Returns:
            DataFrame: A Spark DataFrame containing the parsed Avro data.

        Raises:
            ValueError: If the topic name contains '*' or ',' characters, which are not allowed.
        """
        if "*" in self.topic or "," in self.topic:
            raise ValueError(
                "Topic name cannot contain '*' or ',' characters. Use read_multiple method for multiple topics."
            )

        self.schema_registry.fetch_schema(self.topic, self.schema_version)
        return self.read_raw().transform(
            parse_by_avro(self.topic, self.schema_registry)
        )
