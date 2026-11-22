import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    round as spark_round,
    struct,
    to_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from configs import kafka_config


MY_NAME = "anatolii_zhakun"
INPUT_TOPIC = f"{MY_NAME}_building_sensors"
OUTPUT_TOPIC = f"{MY_NAME}_alerts"
ALERTS_FILE = "alerts_conditions.csv"

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5"


def main():
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{kafka_config["username"]}" '
        f'password="{kafka_config["password"]}";'
    )

    spark = (
        SparkSession.builder
        .appName("IoT sensor alerts")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "Europe/Kyiv")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    sensor_schema = StructType([
        StructField("id", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
    ])

    kafka_options = {
        "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
        "kafka.security.protocol": kafka_config["security_protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
        "kafka.sasl.jaas.config": jaas_config,
    }

    sensor_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    readings = (
        sensor_stream
        .select(from_json(col("value").cast("string"), sensor_schema).alias("data"))
        .select("data.*")
        .withColumn(
            "event_time",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .filter(col("event_time").isNotNull())
    )

    averages = (
        readings
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            col("id"),
            window(col("event_time"), "1 minute", "30 seconds"),
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
        )
        .select(
            col("id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_temperature"),
            col("avg_humidity"),
        )
    )

    alert_conditions = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ALERTS_FILE)
        .withColumnRenamed("id", "alert_id")
    )

    checked = averages.crossJoin(alert_conditions)

    temperature_matches = (
        (
            (col("temperature_min") == -999)
            & (col("temperature_max") == -999)
        )
        | (
            (col("avg_temperature") >= col("temperature_min"))
            & (col("avg_temperature") <= col("temperature_max"))
        )
    )

    humidity_matches = (
        (
            (col("humidity_min") == -999)
            & (col("humidity_max") == -999)
        )
        | (
            (col("avg_humidity") >= col("humidity_min"))
            & (col("avg_humidity") <= col("humidity_max"))
        )
    )

    alerts = (
        checked
        .filter(temperature_matches & humidity_matches)
        .select(
            col("id").cast("string").alias("key"),
            to_json(
                struct(
                    col("id"),
                    col("window_start"),
                    col("window_end"),
                    spark_round(col("avg_temperature"), 2).alias("avg_temperature"),
                    spark_round(col("avg_humidity"), 2).alias("avg_humidity"),
                    col("code"),
                    col("message"),
                )
            ).alias("value"),
        )
    )

    query = (
        alerts.writeStream
        .format("kafka")
        .options(**kafka_options)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", "C:/spark-checkpoints/anatolii_zhakun_alerts")
        .outputMode("append")
        .start()
    )

    print(f"Spark слухає топік {INPUT_TOPIC}")
    print(f"Алерти записуватимуться в {OUTPUT_TOPIC}")
    print("Зупинка: Ctrl+C")

    query.awaitTermination()


if __name__ == "__main__":
    main()
