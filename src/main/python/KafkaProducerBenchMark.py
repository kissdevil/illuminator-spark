from kafka import KafkaProducer
import json
import time

def python_kafka_producer_performance():
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095')

    producer_start = time.time()
    topic = 'kafkaPayloadTest'
    for i in range(10000):
        data = {
            'itemId': i,
            'nerBrand': 'steve'
        }
        producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush() # clear all local buffers and produce pending messages
    return time.time() - producer_start


def calculate_thoughput(timing, n_messages=10000):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

calculate_thoughput(python_kafka_producer_performance())
