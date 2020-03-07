from confluent_kafka.avro import loads
from confluent_kafka.avro import CachedSchemaRegistryClient
from json import dumps
from time import sleep

# currently confluent_kafka does not allow string keys by default
# we have extended the producer class in order to allow it
from custom_confluent_kafka_avro_producer import StringKeyAvroProducer


def update_avro_compatibility(topic_name, compatibility_level, schema_registry_url='http://127.0.0.1:8081'):
    compatibility_levels = ["NONE", "FULL", "FORWARD", "BACKWARD"]
    if compatibility_level.upper() in compatibility_levels:
        schema_registry = CachedSchemaRegistryClient(url=schema_registry_url)
        schema_registry.update_compatibility(level=compatibility_level.upper(), subject=topic_name+"-key")
        schema_registry.update_compatibility(level=compatibility_level.upper(), subject=topic_name+"-value")
    else:
        raise Exception(
            "Compatilibility level not in {}".format(compatibility_levels))


def produce(topic:str,brokers:str,schema_registry_url:str):

    value_schema = loads(dumps({
        "type": "record",
        "namespace": "example.avro", #VERY IMPORTANT TO MAP MESSAGE TO JAVA OBJECT
        "name": "test_record",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "date", "type": ["int", "null"]},
            {"name": "info", "type": "string"}
        ]
    }))

    avro_producer_settings = {
        'bootstrap.servers': brokers,
        'group.id': 'groupid',
        'schema.registry.url': schema_registry_url
    }


    producer = StringKeyAvroProducer(avro_producer_settings)

    i = 0
    while True:
        sleep(1)
        key = "message_key_" + str(i)
        value = {"id": i, "date":  (2+i**2), "info": "sensor_" + str(i)}
        print("Message Produced: key = {} value = {}".format(key, value))
        producer.produce(topic=topic, key=key,value=value,key_schema=None,value_schema=value_schema)
        i += 1
    producer.flush()



###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    topic = "preprocessed"
    schema_registry_url = "http://127.0.0.1:8081"
    brokers = 'localhost:19092'
    try:
        #update_avro_compatibility(topic, "NONE")
        produce(topic,brokers,schema_registry_url)
    except KeyboardInterrupt:
        pass
