import abc
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
from sparkle.config import Config
from sparkle.sources import Source
from sparkle.writer import Writer


@dataclass
class Sources(abc.ABC):
    """Abstract base class for data sources in the Spark application.

    This class is intended to be extended by specific data source implementations.
    """

    pass


class Sparkle(abc.ABC):
    """Base class for Spark applications.

    This is an abstract base class for defining Spark applications.
    It provides a common interface for processing data and writing the results using a set of configurable writers.

    Attributes:
        spark_session (SparkSession): The Spark session used for executing Spark jobs.
        config (Config): Configuration object containing application-specific settings.
        sources (Dict[str, Source]): Dictionary of data sources used in the application, keyed by source name.
        writers (List[Writer]): List of Writer objects used for writing the processed data to various destinations.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        config: Config,
        sources: dict[str, Source],
        writers: list[Writer],
    ):
        """Initializes the Sparkle class with the provided Spark session, configuration, sources, and writers.

        Args:
            spark_session (SparkSession): The Spark session used for executing Spark jobs.
            config (Config): Configuration object containing application-specific settings.
            sources (Dict[str, Source]): Dictionary of data sources used in the application, keyed by source name.
            writers (List[Writer]): List of Writer objects used for writing the processed data to various destinations.
        """
        self.spark_session = spark_session
        self.config = config
        self.sources = sources
        self.writers = writers

    @abc.abstractmethod
    def process(self) -> DataFrame:
        """Abstract method that must be implemented by subclasses to define the processing logic.

        This method should be overridden in subclasses to process the input DataFrames and return a resulting DataFrame.

        Returns:
            DataFrame: The resulting DataFrame after processing.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError("process method must be implemented by subclasses")

    def write(self, df: DataFrame) -> None:
        """Writes the processed DataFrame to the specified destinations using the configured writers.

        The DataFrame is first persisted in memory to optimize writing operations and then
        unpersisted after all writers have completed their tasks.

        Args:
            df (DataFrame): The DataFrame to be written to the destinations.
        """
        df.persist(StorageLevel.MEMORY_ONLY)

        for writer in self.writers:
            writer.write(df)

        df.unpersist()


# Sample usage:
# TODO: Remove after designing
class OrdersApp(Sparkle):
    def process(self) -> DataFrame:
        orders = self.sources["orders"].reader.read()
        customers = self.sources["customers"].reader.read()

        return orders.join(
            customers, orders.customer_id == customers.customer_id, "inner"
        )


spark = SparkSession.builder.appName("OrdersApp").getOrCreate()
config = Config()


@dataclass
class MySources(Sources):
    orders: Source = Source("orders", "some_where", SourceType.hive_table, {})
    customers: Source = Source(
        "customers", "some_where_else", SourceType.hive_table, {}
    )


sources = MySources()


orders_app = OrdersApp(spark, config, sources, [])
result = orders_app.process(sources.orders.reader.read(), sources.customers)
orders_app.write(result)
