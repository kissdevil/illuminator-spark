from kafka import KafkaProducer
import json

broker_host = "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095"
topic = "kafkaPayloadTest"


def sendData(data):
    try:
        producer.send(topic, json.dumps(data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        print('Failed to send average stock price to kafka, caused by: %s' + str(e))
        print(traceback.format_exc())


def initKafkaProducer():
    print("initializing kafka producer...")
    producer = KafkaProducer(bootstrap_servers=broker_host)
    return producer


producer = initKafkaProducer()


