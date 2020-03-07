from confluent_kafka import Producer, KafkaError
from json import dumps
from time import sleep


def json_serialization(x):
    """Encode to bytes 'utf-8"""
    return dumps(x).encode('utf-8')


def produce(brokers:str,topic:str):

    producer_settings = {
        'bootstrap.servers': brokers,
        'group.id': 'groupid'
    }
    producer = Producer(producer_settings)

    i = 0
    while True:
        sleep(1)
        key = "message_key_" + str(i)
        value = {"id": i, "date": 100000 *(2+i**3), "info": "sensor_" + str(i)}
        print("Message Produced: key = {} value = {}".format(key, json_serialization(value)))
        producer.produce(topic=topic, key=key,value=json_serialization(value))
        i += 1
    producer.flush()

###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    brokers = 'localhost:19092'
    topic = "raw"
    try:
        produce(brokers,topic)
    except KeyboardInterrupt:
        pass