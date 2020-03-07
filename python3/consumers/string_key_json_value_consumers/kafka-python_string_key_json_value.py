from kafka import KafkaConsumer  #kafka-python library
from json import dumps
from time import sleep


def json_deserialization(x):
    """Deserializes bytes\n
        If it was an string, obteins an string.\n
        If it was a json string we got an json object (dict).
    """
    return x.decode('utf-8')


def consume_with_kafka_library(brokers:list,topic:str):

    #It could be multiple topics and brokers
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=brokers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='groupid',
                             key_deserializer=json_deserialization,
                             value_deserializer=json_deserialization)

    try:
        for msg in consumer:
            if msg is not None:
                print("Message Consumed: key = {} value = {}".format(msg.key, msg.value))
    except Exception as e:
        print(e)
    finally:
        consumer.close()


###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    brokers = ['localhost:19092']
    topic = "raw"
    try:
        consume_with_kafka_library(brokers,topic)
    except KeyboardInterrupt:
        exit(1)
        pass