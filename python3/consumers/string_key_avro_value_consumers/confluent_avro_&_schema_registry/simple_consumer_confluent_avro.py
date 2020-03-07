# import custom consumer that reads keys as strings
# currently confluent_kafka does not allow string keys by default
# we have extended the consumer class in order to allow it
from custom_confluent_kafka_avro_consumer import StringKeyAvroConsumer

def consume(topic:str,brokers:str,schema_registry_url:str):

    avro_consumer_settings = {
        'bootstrap.servers': brokers,
        'group.id': 'raw_1',
        'client.id': 'client-1',
        'session.timeout.ms': 6000,
        'schema.registry.url': schema_registry_url,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        #'debug' : 'all'
    }

    consumer = StringKeyAvroConsumer(avro_consumer_settings)

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(10.0)
            if msg is None:
                continue
            elif msg.error():
                print("Consumer error: {}".format(msg.error()))
            else:
                print("Message Consumed: key = {} value = {}".format(msg.key(), msg.value()))
    except Exception as e:
        print(e)
    finally:
        consumer.close()



###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    topic = "raw"
    schema_registry_url = "http://127.0.0.1:8081"
    brokers = 'localhost:19092'
    try:
        consume(topic,brokers,schema_registry_url)
    except KeyboardInterrupt:
        exit(1)
        pass
