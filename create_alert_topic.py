from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import kafka_config


TOPIC_NAME = "anatolii_zhakun_alerts"

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

try:
    try:
        admin_client.create_topics(
            new_topics=[
                NewTopic(
                    name=TOPIC_NAME,
                    num_partitions=2,
                    replication_factor=1,
                )
            ]
        )
        print(f"Топік {TOPIC_NAME} створено.")
    except TopicAlreadyExistsError:
        print(f"Топік {TOPIC_NAME} вже існує.")

    print("Перевірка:", TOPIC_NAME in admin_client.list_topics())

finally:
    admin_client.close()