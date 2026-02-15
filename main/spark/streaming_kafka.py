"""Spark Structured Streaming from Kafka for near-real-time sales ingestion."""

from __future__ import annotations

from pyspark.sql import SparkSession


def main(bootstrap_servers: str, topic: str, checkpoint: str, output_path: str) -> None:
    spark = (
        SparkSession.builder.appName("retail-insights-streaming")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    query = (
        stream_df.selectExpr("CAST(value AS STRING) AS payload")
        .writeStream.format("json")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()
    main(args.bootstrap_servers, args.topic, args.checkpoint, args.output_path)
