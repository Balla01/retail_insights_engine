"""Parquet + Delta warehouse and Spark SQL analytics reference."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark(app_name: str = "retail-insights-delta") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "200")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(parquet_path: str, delta_path: str) -> None:
    spark = build_spark()

    df = spark.read.parquet(parquet_path)

    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("txn_date")
        .save(delta_path)
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS retail_transactions
        USING DELTA
        LOCATION '{delta_path}'
        """
    )

    query = """
    SELECT
      txn_date,
      category,
      COUNT(*) AS order_count,
      SUM(amount) AS total_sales
    FROM retail_transactions
    GROUP BY txn_date, category
    ORDER BY txn_date DESC, total_sales DESC
    LIMIT 100
    """
    spark.sql(query).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-path", required=True)
    parser.add_argument("--delta-path", required=True)
    args = parser.parse_args()
    main(args.parquet_path, args.delta_path)
