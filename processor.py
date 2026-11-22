import json

from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config


MY_NAME = "anatolii_zhakun"

SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
TEMPERATURE_TOPIC = f"{MY_NAME}_temperature_alerts"
HUMIDITY_TOPIC = f"{MY_NAME}_humidity_alerts"


consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    key_deserializer=lambda value: value.decode("utf-8") if value else None,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alert_processor",
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

consumer.subscribe([SENSORS_TOPIC])
print(f"Очікуємо дані з топіка {SENSORS_TOPIC}...")

try:
    for record in consumer:
        data = record.value

        print(f"\nОтримано дані: {json.dumps(data, ensure_ascii=False)}")

        sensor_id = data["sensor_id"]
        timestamp = data["timestamp"]
        temperature = data["temperature"]
        humidity = data["humidity"]

        if temperature > 40:
            alert = {
                **data,
                "alert_type": "temperature",
                "message": f"Температура {temperature}°C перевищила поріг 40°C",
            }

            result = producer.send(
                TEMPERATURE_TOPIC,
                key=sensor_id,
                value=alert,
            ).get(timeout=10)

            print(f"Відфільтровано: температура {temperature}°C > 40°C")
            print(f"Сповіщення надіслано в {result.topic}: {alert['message']}")

        if humidity > 80 or humidity < 20:
            alert = {
                **data,
                "alert_type": "humidity",
                "message": f"Вологість {humidity}% поза допустимими межами 20–80%",
            }

            result = producer.send(
                HUMIDITY_TOPIC,
                key=sensor_id,
                value=alert,
            ).get(timeout=10)

            print(f"Відфільтровано: вологість {humidity}% поза межами 20–80%")
            print(f"Сповіщення надіслано в {result.topic}: {alert['message']}")

except KeyboardInterrupt:
    print("\nОбробку зупинено.")

finally:
    consumer.close()
    producer.flush()
    producer.close()