import boto3
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json

KEYS = []

def from_s3(bucket_name: str, file_name: str) -> str:
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, file_name)

    body = obj.get()['Body'].read()
    return body.decode("utf-8")

def create_topic(topic_name: str):
    admin_client = KafkaAdminClient(
        bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094",
                           "b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094"],
        client_id='tutors',
        security_protocol="SSL",
        ssl_cafile="truststore.pem"
    )

    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=2))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    admin_client.list_topics()

def to_kafka(topic_name: str) -> list:
    producer = KafkaProducer(
        bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094",
                           "b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094"],
        security_protocol="SSL",
        ssl_cafile="truststore.pem",
        # ssl_certfile=cert_folder + "/service.cert",
        # ssl_keyfile=cert_folder + "/service.key",
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 0)
    )

    txt_file = from_s3("tutors-kafka", "transcriptions/Amharic_transcriptions/Clean_Amharic.txt")
    lines = txt_file.splitlines()
    for line in lines:
        key = line[line.find("(") + 1: line.find(")")]
        sentence = line[: line.find("(")]

        print("Sending: {}".format(key))
        print(f"Sentence: {sentence}")
        # sending the message to Kafka
        producer.send(topic_name,
                      key=key,
                      value=sentence)

        KEYS.append(key.split(" ")[1])

    producer.flush()
    producer.close()

if __name__ == '__main__':
    # create_topic("Jabor047_text_topic")
    to_kafka("Jabor047_text_topic")
