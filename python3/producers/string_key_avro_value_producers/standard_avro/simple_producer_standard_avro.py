from confluent_kafka import Producer, KafkaError
from json import dumps
from time import sleep
from avro.schema import Parse #avro-python3 library
from avro.io import DatumWriter,BinaryEncoder
from io import BytesIO


def json_serialization(x):
    """Encode to bytes 'utf-8"""
    return dumps(x).encode('utf-8')


def avro_serialization(value_schema, x):
    bytes_writer = BytesIO()
    datum_writer = DatumWriter(writer_schema=value_schema)
    encoder = BinaryEncoder(bytes_writer)
    datum_writer.write_data(value_schema,x, encoder)
    serialized_x = bytes_writer.getvalue()
    bytes_writer.close()
    return serialized_x



def produce(brokers: str, topic: str):

    value_schema = Parse(dumps({
        "type": "record",
        "namespace": "example.avro", #VERY IMPORTANT TO MAP MESSAGE TO JAVA OBJECT
        "name": "test_record",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "date", "type": ["int", "null"]},
            {"name": "info", "type": "string"}
        ]
    }))

    producer_settings = {'bootstrap.servers': brokers, 'group.id': 'groupid'}
    producer = Producer(producer_settings)

    i = 0
    while True:
        sleep(1)
        key = "message_key_" + str(i)
        value = {
            "id": i,
            "date": 100000 * (2 + i**3),
            "info": "sensor_" + str(i)
        }
        print("Message Produced: key = {} value = {}".format(key, value))
        producer.produce(topic=topic, key=key, value=avro_serialization(value_schema,value))
        i += 1
    producer.flush()


###############################################################################################################################
##################################################### MAIN ####################################################################
###############################################################################################################################
if __name__ == "__main__":
    brokers = 'localhost:19092'
    topic = "raw"
    try:
        produce(brokers, topic)
    except KeyboardInterrupt:
        pass