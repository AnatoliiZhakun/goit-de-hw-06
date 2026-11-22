from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import kafka_config


MY_NAME = "anatolii_zhakun"

topic_names = [
    f"{MY_NAME}_building_sensors",
    f"{MY_NAME}_temperature_alerts",
    f"{MY_NAME}_humidity_alerts",
]

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

try:
    topics_to_create = [
        NewTopic(name=name, num_partitions=2, replication_factor=1)
        for name in topic_names
    ]

    try:
        admin_client.create_topics(
            new_topics=topics_to_create,
            validate_only=False,
        )
        print("Топіки створено.")
    except TopicAlreadyExistsError:
        print("Принаймні один топік уже існує; перевіряємо список.")

    print("Топіки з вашим ідентифікатором:")
    [print(topic) for topic in admin_client.list_topics()
     if MY_NAME in topic]

finally:
    admin_client.close()