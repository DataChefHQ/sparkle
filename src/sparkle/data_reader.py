from pyspark.sql import DataFrame, SparkSession
from sparkle.config import Config


class KafkaReader:
    # TODO
    pass


class TableReader:
    # TODO
    pass


class DataReader:
    def __init__(
        self,
        entity_name: str,
        kafka_reader: None | KafkaReader = None,
        table_reader: None | TableReader = None,
        spark: SparkSession | None = SparkSession.getActiveSession(),
    ):
        if not spark:
            raise Exception("No Spark session is provided or discoverable.")

        self.entity_name = entity_name
        self.kafka_reader = kafka_reader
        self.table_reader = table_reader
        self.spark = spark

    @classmethod
    def with_config(
        cls, entity_name: str, config: Config, spark: SparkSession
    ) -> "DataReader":
        return cls(
            entity_name=entity_name,
            kafka_reader=KafkaReader.with_config(config=config, spark=spark),
            table_reader=TableReader.with_config(config=config, spark=spark),
            spark=spark,
        )

    def stream(self, avro: bool = False) -> DataFrame:
        if not self.kafka_reader:
            raise Exception("No Kafka reader configuration found!")
        if avro:
            # TODO
            if not self.kafka_reader.schema_registry:
                raise Exception("Kafka reader doesn't support schema registry")
            return self.kafka_reader.read_avro(topic=self.entity_name)
        else:
            return self.kafka_reader.read_data(topic=self.entity_name)

    def batch(
        self,
    ) -> DataFrame:
        if not self.table_reader:
            raise Exception("No table reader configuration found!")
        return self.table_reader.read(table_name=self.entity_name)
