import json
from kafka import KafkaConsumer
from flask import Flask, render_template

app = Flask(__name__)

def consumer_gen():
    consumer = KafkaConsumer(
        "Jabor047_text_topic",
        group_id="tutors",
        bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094",
                           "b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094"],
        security_protocol="SSL",
        ssl_cafile="truststore.pem",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

    for message in consumer:
        yield (message.offset, message.value)

text_gen = consumer_gen()
@app.route("/")
def index():
    return(render_template('index_2.html', data=next(text_gen)))

def next_gen():
    next(text_gen)

if __name__ == "__main__":
    app.run(debug=True, port=5010)
