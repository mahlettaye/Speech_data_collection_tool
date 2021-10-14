from kafka import KafkaConsumer

def consume_transcriptions(topic_name:str):
    consumer = KafkaConsumer(topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=-1)
        
    for message in consumer:
        message = message.value.decode('utf-8')
        # collection.insert_one(message)
        print('{} returned from s3'.format(message))

if __name__ == '__main__':
    consume_transcriptions(topic_name='s3-test-topic')