from typing import Any

from pyspark.sql import DataFrame, SparkSession

from sparkle.config import Config
from sparkle.utils.spark import to_kafka_dataframe
from sparkle.writer import Writer


class KafkaStreamPublisher(Writer):
    """KafkaStreamPublisher class for writing DataFrames to Kafka streams.

    Inherits from the Writer abstract base class and implements the write
    method for writing data to Kafka topics.

    Args:
        kafka_options (dict[str, Any]): Kafka connection options.
        checkpoint_location (str): Location for checkpointing streaming data.
        kafka_topic (str): Kafka topic to which data will be written.
        output_mode (str): Mode for writing data ('append', 'complete', etc.).
        unique_identifier_column_name (str): Column name used as the Kafka key.
        spark (SparkSession): Spark session instance to use.
        trigger_once (bool): Whether to trigger the stream once and then stop.
    """

    def __init__(
        self,
        kafka_options: dict[str, Any],
        checkpoint_location: str,
        kafka_topic: str,
        output_mode: str,
        unique_identifier_column_name: str,
        spark: SparkSession,
        trigger_once: bool = True,
    ) -> None:
        """Initialize the KafkaStreamPublisher object.

        Args:
            kafka_options (dict[str, Any]): Kafka options for the connection.
            checkpoint_location (str): Path for checkpointing streaming progress.
            kafka_topic (str): The target Kafka topic for writing data.
            output_mode (str): Output mode for writing data.
            unique_identifier_column_name (str): Column name to be used as Kafka key.
            spark (SparkSession): The Spark session to be used for writing data.
            trigger_once (bool, optional): Whether to trigger the stream once. Defaults to True.
        """
        super().__init__(spark)
        self.kafka_options = kafka_options
        self.checkpoint_location = checkpoint_location
        self.kafka_topic = kafka_topic
        self.output_mode = output_mode
        self.unique_identifier_column_name = unique_identifier_column_name
        self.trigger_once = trigger_once

    @classmethod
    def with_config(cls, config: Config, spark: SparkSession, **kwargs: Any) -> "KafkaStreamPublisher":
        """Create a KafkaStreamPublisher object with a configuration.

        Args:
            config (Config): Configuration object containing settings for the writer.
            spark (SparkSession): The Spark session to be used for writing data.
            **kwargs (Any): Additional keyword arguments.

        Returns:
            KafkaStreamPublisher: An instance configured with the provided settings.
        """
        if not config.kafka_output:
            raise ValueError("Kafka output configuration is not provided")

        c = config.kafka_output

        return cls(
            kafka_options=c.kafka_config.spark_kafka_config,
            checkpoint_location=config.checkpoint_location,
            kafka_topic=c.kafka_topic,
            output_mode=c.output_mode,
            unique_identifier_column_name=c.unique_identifier_column_name,
            spark=spark,
            trigger_once=c.trigger_once,
        )

    def write(self, df: DataFrame) -> None:
        """Write DataFrame to Kafka by converting it to JSON using the configured primary key.

        This method transforms the DataFrame using the unique identifier column name
        and writes it to the configured Kafka topic.

        Args:
            df (DataFrame): The DataFrame to be written.

        Raises:
            KeyError: If the DataFrame does not have the required 'key' and 'value' columns.
        """
        # Convert the DataFrame to a Kafka-friendly format
        kafka_df = to_kafka_dataframe(self.unique_identifier_column_name, df)

        if "key" not in kafka_df.columns or "value" not in kafka_df.columns:
            raise KeyError(
                "The DataFrame must contain 'key' and 'value' columns. "
                "Ensure that `to_kafka_dataframe` transformation is correctly applied."
            )

        (
            kafka_df.writeStream.format("kafka")
            .outputMode(self.output_mode)
            .option("checkpointLocation", self.checkpoint_location)
            .options(**self.kafka_options)
            .trigger(once=self.trigger_once)
            .option("topic", self.kafka_topic)
            .start()
        )


class KafkaBatchPublisher(Writer):
    """KafkaBatchublisher class for writing DataFrames in batch to Kafka.

    Inherits from the Writer abstract base class and implements the write
    method for writing data to Kafka topics.

    Args:
        kafka_options (dict[str, Any]): Kafka connection options.
        kafka_topic (str): Kafka topic to which data will be written.
        unique_identifier_column_name (str): Column name used as the Kafka key.
        spark (SparkSession): Spark session instance to use.
    """

    def __init__(
        self,
        kafka_options: dict[str, Any],
        kafka_topic: str,
        unique_identifier_column_name: str,
        spark: SparkSession,
    ) -> None:
        """Initialize the KafkaBatchPublisher object.

        Args:
            kafka_options (dict[str, Any]): Kafka options for the connection.
            kafka_topic (str): The target Kafka topic for writing data.
            unique_identifier_column_name (str): Column name to be used as Kafka key.
            spark (SparkSession): The Spark session to be used for writing data.
        """
        super().__init__(spark)
        self.kafka_options = kafka_options
        self.kafka_topic = kafka_topic
        self.unique_identifier_column_name = unique_identifier_column_name

    @classmethod
    def with_config(cls, config: Config, spark: SparkSession, **kwargs: Any) -> "KafkaBatchPublisher":
        """Create a KafkaBatchPublisher object with a configuration.

        Args:
            config (Config): Configuration object containing settings for the writer.
            spark (SparkSession): The Spark session to be used for writing data.
            **kwargs (Any): Additional keyword arguments.

        Returns:
            KafkaBatchPublisher: An instance configured with the provided settings.
        """
        if not config.kafka_output:
            raise ValueError("Kafka output configuration is missing")

        c = config.kafka_output

        return cls(
            kafka_options=c.kafka_config.spark_kafka_config,
            kafka_topic=c.kafka_topic,
            unique_identifier_column_name=c.unique_identifier_column_name,
            spark=spark,
        )

    def write(self, df: DataFrame) -> None:
        """Write DataFrame to Kafka by converting it to JSON using the configured primary key.

        This method transforms the DataFrame using the unique identifier column name
        and writes it to the configured Kafka topic.

        Args:
            df (DataFrame): The DataFrame to be written.

        Raises:
            KeyError: If the DataFrame does not have the required 'key' and 'value' columns.
        """
        # Convert the DataFrame to a Kafka-friendly format
        kafka_df = to_kafka_dataframe(self.unique_identifier_column_name, df)

        if "key" not in kafka_df.columns or "value" not in kafka_df.columns:
            raise KeyError(
                "The DataFrame must contain 'key' and 'value' columns. "
                "Ensure that `to_kafka_dataframe` transformation is correctly applied."
            )

        # fmt: off
        (
            kafka_df.write.format("kafka")
            .options(**self.kafka_options)
            .option("topic", self.kafka_topic)
            .save()
        )
        # fmt: on
