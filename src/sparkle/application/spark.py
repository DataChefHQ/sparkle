import os
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from sparkle.utils.logger import logger

try:
    from awsglue.context import GlueContext
except ImportError:
    logger.warning("Could not import pyspark. This is expected if running locally.")

_SPARK_EXTENSIONS = [
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
]
_SPARK_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.apache.spark:spark-avro_2.12:3.3.0",
]


def get_local_session():
    """Create and return a local Spark session configured for use with Iceberg and Kafka.

    This function sets up a local Spark session with specific configurations for Iceberg
    catalog, session extensions, and other relevant settings needed for local testing
    and development. It supports optional custom Ivy settings for managing dependencies.

    Returns:
        SparkSession: A configured Spark session instance for local use.
    """
    ivy_settings_path = os.environ.get("IVY_SETTINGS_PATH", None)
    LOCAL_CONFIG = {
        "spark.sql.extensions": ",".join(_SPARK_EXTENSIONS),
        "spark.jars.packages": ",".join(_SPARK_PACKAGES),
        "spark.sql.jsonGenerator.ignoreNullFields": False,
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": "./tmp/warehouse",
        "spark.sql.defaultCatalog": "local",
    }

    spark_conf = SparkConf()

    for key, value in LOCAL_CONFIG.items():
        spark_conf.set(key, str(value))

    spark_session = (
        SparkSession.builder.master("local[*]")
        .appName("LocalSparkleApp")
        .config(conf=spark_conf)
    )

    if ivy_settings_path:
        spark_session.config("spark.jars.ivySettings", ivy_settings_path)

    return spark_session.getOrCreate()


def get_glue_session(warehouse_location: str):
    """Create and return a Glue session configured for use with Iceberg and AWS Glue Catalog.

    This function sets up a Spark session integrated with AWS Glue, using configurations
    suitable for working with Iceberg tables stored in AWS S3. It configures Spark with
    AWS Glue-specific settings, including catalog implementation and S3 file handling.

    Args:
        warehouse_location (str): The S3 path to the warehouse location for Iceberg tables.

    Returns:
        SparkSession: A configured Spark session instance integrated with AWS Glue.
    """
    GLUE_CONFIG = {
        "spark.sql.extensions": ",".join(_SPARK_EXTENSIONS),
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": warehouse_location,
        "spark.sql.jsonGenerator.ignoreNullFields": False,
    }

    spark_conf = SparkConf()

    for key, value in GLUE_CONFIG.items():
        spark_conf.set(key, str(value))

    glueContext = GlueContext(SparkContext.getOrCreate(conf=spark_conf))
    return glueContext.spark_session
