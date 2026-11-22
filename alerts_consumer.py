import json

from kafka import KafkaConsumer
from configs import kafka_config


MY_NAME = "anatolii_zhakun"

TOPICS = [
    f"{MY_NAME}_temperature_alerts",
    f"{MY_NAME}_humidity_alerts",
]

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alerts_viewer",
)

consumer.subscribe(TOPICS)
print(f"Очікуємо сповіщення з топіків: {', '.join(TOPICS)}")

try:
    for record in consumer:
        alert = json.loads(record.value.decode("utf-8"))

        print(
            f"[{record.topic}] "
            f"Час: {alert['timestamp']} | "
            f"Сенсор: {alert['sensor_id']} | "
            f"{alert['message']}"
        )

except KeyboardInterrupt:
    print("\nПерегляд сповіщень зупинено.")

finally:
    consumer.close()