from kafka import KafkaProducer  #kafka-python library
from json import dumps
from time import sleep


## ACTIVATE DEBUG
# import logging
# import sys
# logger = logging.getLogger('kafka')
# logger.addHandler(logging.StreamHandler(sys.stdout))
# logger.setLevel(logging.DEBUG)


def json_serialization(x):
    """Converts to json string and encode it to bytes in 'utf-8'"""
    return dumps(x).encode('utf-8')

def string_to_bytes(x):
    """Converts to bytes"""
    return x.encode('utf-8')


def produce(brokers:list,topic:str):

    producer = KafkaProducer(bootstrap_servers=brokers,
                            key_serializer=string_to_bytes,
                            value_serializer=json_serialization)

    i = 0
    while i<1000:
        sleep(1)
        key = "message_key_" + str(i)
        value = {"id": i, "date": 100000 *(2+i**3), "info": "sensor_" + str(i)}
        print("Message Produced: key = {} value = {}".format(key, value))
        producer.send(topic=topic, key=key, value=value)
        i += 1
    producer.flush()
    producer.close()

###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    brokers = ['localhost:19092']
    topic = "raw"
    try:
        produce(brokers,topic)
    except KeyboardInterrupt:
        pass