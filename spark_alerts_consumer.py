import json

from kafka import KafkaConsumer
from configs import kafka_config


TOPIC_NAME = "anatolii_zhakun_alerts"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    group_id="anatolii_zhakun_spark_alerts",
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Очікуємо алерти з топіка {TOPIC_NAME}...")

try:
    for record in consumer:
        alert = record.value
        print(
            f"[{alert['code']}] {alert['message']} | "
            f"Сенсор: {alert['id']} | "
            f"Середня температура: {alert['avg_temperature']}°C | "
            f"Середня вологість: {alert['avg_humidity']}% | "
            f"Вікно: {alert['window_start']} — {alert['window_end']}"
        )
except KeyboardInterrupt:
    print("\nЧитання алертів зупинено.")
finally:
    consumer.close()