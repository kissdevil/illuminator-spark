from kafka import KafkaProducer
import json

TARGET_HOST = "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095"
TARGET_TOPIC = "cqibrandstreamingflow"


def sendData(data):
    try:
        producer.send(TARGET_TOPIC, json.dumps(data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        print('Failed to send ner message to cqibrand main flow kafka, caused by: %s' + str(e))
        print(traceback.format_exc())


def initKafkaProducer():
    print("initializing kafka producer...")
    producer = KafkaProducer(bootstrap_servers=TARGET_HOST)
    return producer


producer = initKafkaProducer()


