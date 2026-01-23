> 🚧 **Project Status**
>
> We at **DataChef** are currently focused on building and shipping new products, which means this repository is not actively maintained at the moment.
>
> That said, we’re always happy to connect.  
> • Curious about what we’re working on now? Visit [our website](https://datachef.co) to see our latest projects.  
> • Interested in this repository or thinking about taking it further? Feel free to reach out to [us directly](https://www.linkedin.com/company/datachefco/).
>
> Thanks for stopping by, and happy building.

# Sparkle ✨

**Sparkle** is a meta-framework built on top of [Apache
Spark](https://spark.apache.org/), designed to streamline data
engineering workflows and accelerate the delivery of data
products. Developed by [**DataChef**](https://datachef.co), Sparkle
focuses on three main areas:

1. **Improving Developer Experience (DevEx) 🚀**
2. **Reducing Time to Market ⏱️**
3. **Easy Maintenance 🔧**

With these goals in mind, Sparkle has enabled DataChef to deliver
functional data products from day one, allowing for seamless handovers
to internal teams.

Read more about Sparkle on [DataChef's blog!](https://blog.datachef.co/sparkle-accelerating-data-engineering-with-datachefs-meta-framework)

## Key Features

### 1. Improved Developer Experience 🚀

Sparkle enhances the developer experience by abstracting away
non-business-critical aspects of Spark application development. It
achieves this through:

- **Sophisticated Configuration Mechanism**: Simplifies the setup and
  configuration of Spark applications, allowing developers to focus
  solely on business logic.
- **Automatic Functional Tests 🧪**: Generates tests for each
  application automatically, based on predefined input and output
  fixtures. This ensures that the application behaves as expected
  without requiring extensive manual testing.

### 2. Reduced Time to Market ⏱️

Sparkle significantly reduces the time to market by automating the
deployment and testing processes. This allows data engineers to
concentrate exclusively on developing the business logic, with all
other aspects handled by Sparkle:

- **Automated Testing ✅**: Ensures that all applications are robust
  and ready for deployment without manual intervention.
- **Seamless Deployment 🚢**: Automates the deployment pipeline,
  reducing the time needed to bring new data products to market.

### 3. Enhanced Maintenance 🔧

Sparkle simplifies maintenance through heavy testing and abstraction
of non-business functional requirements. This provides a reliable and
trustworthy system that is easy to maintain:

- **Abstraction of Non-Business Logic 📦**: By focusing on business
  logic, Sparkle minimizes the complexity associated with maintaining
  Spark applications.
- **Heavily Tested Framework 🔍**: All non-business functionalities
  are thoroughly tested, reducing the risk of bugs and ensuring a
  stable environment for data applications.

## How It Works 🛠️

The Sparkle framework operates on a principle similar to Function as a
Service (FaaS). Developers can instantiate a Sparkle application that
takes a list of input DataFrames and focuses solely on transforming
these DataFrames according to the business logic. The Sparkle
application then automatically writes the output of this
transformation to the desired destination.

Sparkle follows a streamlined approach, designed to reduce effort in
data transformation workflows. Here’s how it works:

1. **Specify Input Locations and Types**: Easily set up input locations
and types for your data. Sparkle’s configuration makes this effortless,
removing typical setup hurdles and letting you get started
with minimal overhead.

    ```python
    ...
    config=Config(
      ...,
      kafka_input=KafkaReaderConfig(
                        KafkaConfig(
                            bootstrap_servers="localhost:9119",
                            credentials=Credentials("test", "test"),
                        ),
                        kafka_topic="src_orders_v1",
                    )
    ),
    readers={"orders": KafkaReader},
    ...
    ```

2. **Define Business Logic**: This is where developers spend most of their time.
Using Sparkle, you create transformations on input DataFrames, shaping data
according to your business needs.

    ```python
    # Override process function from parent class
    def process(self) -> DataFrame:
            return self.input["orders"].read().join(
                self.input["users"].read()
            )
    ```

3. **Specify Output Locations**: Sparkle automatically writes transformed data to
the specified output location, streamlining the output step to make data
available wherever it’s needed.

    ```python
    ...
    config=Config(
      ...,
      iceberg_output=IcebergConfig(
                        database_name="all_products",
                        table_name="orders_v1",
                    ),
    ),
    writers=[IcebergWriter],
    ...
    ```


This structure lets developers concentrate on meaningful transformations while
Sparkle takes care of configurations, testing, and output management.

## Connectors 🔌

Sparkle offers specialized connectors for common data sources and sinks,
making data integration easier. These connectors are designed to
enhance—not replace—the standard Spark I/O options,
streamlining development by automating complex setup requirements.

### Readers

1. **Iceberg Reader**: Simplifies reading from Iceberg tables,
making integration with Spark workflows a breeze.

2. **Kafka Reader (with Avro schema registry)**: Ingest streaming data
from Kafka with seamless Avro schema registry integration, supporting
data consistency and schema evolution.

### Writers

1. **Iceberg Writer**: Easily write transformed data to Iceberg tables,
ideal for time-traveling, partitioned data storage.

2. **Kafka Writer**: Publish data to Kafka topics with ease, supporting
real-time analytics and downstream consumers.

## Getting Started 🚀

Sparkle is currently under heavy development, and we are continuously
working on improving and expanding its capabilities.

To stay updated on our progress and access the latest information,
follow us on [LinkedIn](https://nl.linkedin.com/company/datachefco)
and [GitHub](https://github.com/DataChefHQ/Sparkle).

## Example

This is the simplest example to create a Orders pipelines by reading records
from a Kafka topic and writing it to an Iceberg table:

```python
from sparkle.config import Config, IcebergConfig, KafkaReaderConfig
from sparkle.config.kafka_config import KafkaConfig, Credentials
from sparkle.writer.iceberg_writer import IcebergWriter
from sparkle.application import Sparkle
from sparkle.reader.kafka_reader import KafkaReader

from pyspark.sql import DataFrame


class CustomerOrders(Sparkle):
  def __init__(self):
      super().__init__(
          config=Config(
              app_name="orders",
              app_id="orders-app",
              version="0.0.1",
              database_bucket="s3://test-bucket",
              checkpoints_bucket="s3://test-checkpoints",
              iceberg_output=IcebergConfig(
                  database_name="all_products",
                  table_name="orders_v1",
              ),
              kafka_input=KafkaReaderConfig(
                  KafkaConfig(
                      bootstrap_servers="localhost:9119",
                      credentials=Credentials("test", "test"),
                  ),
                  kafka_topic="src_orders_v1",
              ),
          ),
          readers={"orders": KafkaReader},
          writers=[IcebergWriter],
      )

  def process(self) -> DataFrame:
      return self.input["orders"].read()
```

## Contributing 🤝

We welcome contributions from the community! If you're interested in
contributing to Sparkle, please check our [GitHub
repository](https://github.com/DataChefHQ/Sparkle) for more details on
how you can get involved.

## License 📄

Sparkle is licensed under the Apache v2.0 License. See the
[LICENSE](LICENSE) file for more details.

## Contact 📬

For more information, questions, or feedback, feel free to reach out
to us on [LinkedIn](https://nl.linkedin.com/company/datachefco) or
open an issue on our
[GitHub](https://github.com/DataChefHQ/sparkle/issues) repository.

---

Thank you for your interest in Sparkle! We're excited to have you join
us on this journey to revolutionize data engineering with Apache
Spark. 🎉
