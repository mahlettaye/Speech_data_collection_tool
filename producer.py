from kafka import KafkaProducer
from json import dumps
from time import sleep
from insert_into_s3 import read_transcription_files


KEYS = []

def parse_sentence():
    sentences = read_transcription_files(bucket_name='tutors-kafka', 
            filename='transcriptions/Amharic_transcriptions/myFile.csv')
    for i in sentences['name'].to_json():
        print(sentences[i])
        break
 
    return sentences['name'].to_json()
        

def send_sentence(topic_name:str='numtest'):
    "simple kafka producer"
    producer= KafkaProducer(bootstrap_servers='localhost:9092')

    producer.send(topic_name, value=parse_sentence().encode() )

    producer.flush()
    sleep(1)

if __name__ == "__main__":
    send_sentence(topic_name='s3-test-topic')
    # parse_sentence()
