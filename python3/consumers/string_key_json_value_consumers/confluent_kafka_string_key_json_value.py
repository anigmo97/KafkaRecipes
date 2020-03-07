from confluent_kafka import Consumer, KafkaError


def json_deserialization(x):
    """Deserializes bytes\n
        If it was an string, obteins an string.\n
        If it was a json string we got an json object (dict).
    """
    return x.decode('utf-8')


def consume_with_confluent_kafka_library(brokers:str,topic:str):

    consumer_settings = {
        'bootstrap.servers': brokers,
        'group.id': 'raw_1',
        'client.id': 'client-1',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
        #,'debug': 'consumer' #activate debug on consumer side
    }
    consumer = Consumer(consumer_settings)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(10000.0)
            if msg is None:
                continue
            elif msg.error():
                print("Consumer error: {}".format(msg.error()))
            else:
                print("Message Consumed: key = {} value = {}".format(
                    json_deserialization(msg.key()), 
                    json_deserialization(msg.value())))
    except Exception as e:
        print(e)
    finally:
        consumer.close()

###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    brokers = 'localhost:19092'
    topic = "raw"
    try:
        consume_with_confluent_kafka_library(brokers,topic)
    except KeyboardInterrupt:
        exit(1)
        pass