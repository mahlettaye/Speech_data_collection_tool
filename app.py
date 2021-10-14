from flask import Flask, Response, render_template,request, stream_with_context
from pykafka import KafkaClient
import os
import json
from kafka import KafkaConsumer

app = Flask(__name__)

def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

def gen_transcription():
    consumer = KafkaConsumer(
        "KevTopic",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for msg in consumer:
        print(msg.value)
        yield msg.value

transcription = gen_transcription()


@app.route("/", methods=['POST', 'GET'])
def index():
    if request.method == "POST":
        counter=1
        if os.path.isfile('./Audio'+str(counter)+'.wav'):
            counter= int(''.join([n for n in './Audio'+str(counter) if n.isdigit()]))
            counter=counter+1
            f = open('./Audio'+str(counter)+'.wav', 'wb')
            f.write(request.get_data("audio_data"))
            f.close()

        return( render_template("index.html", request="POST") )
    else:
        return( render_template("index.html", data = next(transcription)))


@app.route('/topic/<topicname>')
def get_messages(topicname):
    client = get_kafka_client()

    @stream_with_context
    def events():
        for i in client.topics[topicname].get_simple_consumer():
            yield '{0}\n'.format(i.value.decode())
    return (Response(events()))

if __name__ == "__main__":
    app.run(debug=True, port=5001)
