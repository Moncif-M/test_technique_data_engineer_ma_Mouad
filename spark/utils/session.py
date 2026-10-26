"""Spark session factory configured for Delta Lake and the S3-compatible lake."""

import os

from pyspark.sql import SparkSession

from utils.log import get_logger

logger = get_logger(__name__)

REQUIRED_ENV = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")


def get_spark(app_name: str) -> SparkSession:
    """Build or reuse a local Spark session wired to Delta and S3A.

    Raises:
        RuntimeError: if the S3 credentials are missing from the environment.
    """
    missing = [name for name in REQUIRED_ENV if not os.getenv(name)]
    if missing:
        raise RuntimeError("Missing required environment variables for S3 access: " + ", ".join(missing))

    logger.info("creating Spark session %s", app_name)
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3_ENDPOINT", "http://silo:9000"),
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
