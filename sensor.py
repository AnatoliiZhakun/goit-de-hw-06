import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from configs import kafka_config


TOPIC_NAME = "anatolii_zhakun_building_sensors"

# ID сталий протягом одного запуску сенсора.
sensor_id = random.randint(100000, 999999)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Сенсор запущено. ID сенсора: {sensor_id}")

try:
    while True:
        reading = {
            "id": sensor_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "temperature": round(random.uniform(25, 45), 1),
            "humidity": round(random.uniform(15, 85), 1),
        }

        metadata = producer.send(
            TOPIC_NAME,
            key=sensor_id,
            value=reading,
        ).get(timeout=10)

        print(
            f"Надіслано: {json.dumps(reading, ensure_ascii=False)} "
            f"| topic={metadata.topic}, partition={metadata.partition}"
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("\nРоботу сенсора зупинено.")

finally:
    producer.flush()
    producer.close()