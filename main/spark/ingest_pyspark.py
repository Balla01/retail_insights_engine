"""Batch ingestion + cleaning for 100GB+ sales data with PySpark."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, trim, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def build_spark(app_name: str = "retail-insights-ingestion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def main(raw_path: str, parquet_output: str) -> None:
    spark = build_spark()

    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("category", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("region", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )

    df = (
        spark.read.option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(raw_path)
    )

    clean = (
        df.withColumn("order_id", trim(col("order_id")))
        .withColumn("date_ts", to_timestamp(col("date")))
        .withColumn("txn_date", to_date(col("date_ts")))
        .withColumn("qty", col("qty").cast(DoubleType()))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
        .filter(col("order_id").isNotNull())
        .filter(col("amount").isNotNull())
        .dropDuplicates(["order_id"])
    )

    clean.write.mode("overwrite").partitionBy("txn_date").parquet(parquet_output)
    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-path", required=True, help="Glob path like /data/raw/*.csv")
    parser.add_argument("--parquet-output", required=True, help="Target parquet path")
    args = parser.parse_args()
    main(args.raw_path, args.parquet_output)
