from json import dumps
from time import sleep
from avro.schema import Parse #avro-python3 library
from avro.io import DatumReader,BinaryDecoder
from io import BytesIO

from confluent_kafka import Consumer, KafkaError



def json_deserialization(x):
    """Deserializes bytes\n
        If it was an string, obteins an string.\n
        If it was a json string we got an json object (dict).
    """
    return x.decode('utf-8')

def avro_deserialization(value_schema,x):
    datum_reader = DatumReader(writer_schema=value_schema,
                            reader_schema=value_schema)
    bytes_io = BytesIO(x)
    decoder = BinaryDecoder(bytes_io)
    deserialized_x = datum_reader.read(decoder)
    bytes_io.close()
    return deserialized_x





def consume(brokers: str, topic: str):

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
                deserialized_value = avro_deserialization(value_schema,msg.value())
                print("Message Consumed: key = {} value = {}".format(
                    json_deserialization(msg.key()), deserialized_value))
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
        consume(brokers, topic)
    except KeyboardInterrupt:
        exit(1)
        pass