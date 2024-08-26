from pyspark.sql import DataFrame

from sparkle import Sparkle
from sparkle.data_reader import IcebergReader, KafkaReader, MultiDataReader
from sparkle.data_writer import MultiDataWriter, IcebergWriter, KafkaWriter
from sparkle.models import InputField, Trigger, TriggerMethod


app = Sparkle(
    reader=MultiDataReader(
        IcebergReader(database_name="db1"),
        KafkaReader(server="localhost:9092"),
    ),
    writer=MultiDataWriter(
        IcebergWriter(database_name="db1"),
        IcebergWriter(database_name="db2"),
        KafkaWriter(server="localhost:9092"),
    ),
)


@app.pipeline(
    name="orders",
    inputs=[
        InputField("new_orders", KafkaReader, topic="com-sap-new-orders-v0.1"),
        InputField("history_orders", IcebergReader, table="orders-v0.1"),
    ],
)
def orders(
    trigger: Trigger, new_orders: DataFrame, history_orders: DataFrame
) -> DataFrame:
    """The pipeline to process the new and historical orders."""
    match trigger.method:
        case TriggerMethod.PROCESS:
            return new_orders.union(history_orders)
        case TriggerMethod.REPROCESS:
            return new_orders
        case _:
            raise ValueError(f"Unsupported trigger method: {trigger.method}")
