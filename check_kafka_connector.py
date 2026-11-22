from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("KafkaConnectorCheck")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5",
    )
    .getOrCreate()
)

spark._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(
    "org.apache.spark.sql.kafka010.KafkaSourceProvider"
)

print(f"Spark version: {spark.version}")
print("Kafka connector завантажено.")

spark.stop()