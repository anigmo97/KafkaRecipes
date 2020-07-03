from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import (SerializerError,KeySerializerError,ValueSerializerError)
from confluent_kafka.avro.error import ClientError

from confluent_kafka import Producer




class StringKeyAvroProducer(AvroProducer):
    """
        Kafka Producer client which does avro schema encoding to messages values only.
        Handles schema registration, Message serialization.
        The message key is an String.
        Constructor takes below parameters.
        :param dict config: Config parameters containing url for schema registry (``schema.registry.url``)
                            and the standard Kafka client configuration (``bootstrap.servers`` et.al).

        :param str default_value_schema: Optional default avro schema for value
    """
    def __init__(self,
                 config,
                 default_value_schema=None,
                 schema_registry=None):
        invalid_properties = ["schema.registry.url"]
        self.default_producer = Producer(
            {k: v
             for k, v in config.items() if k not in invalid_properties})
        super(StringKeyAvroProducer,
              self).__init__(config,
                             default_value_schema=default_value_schema,
                             schema_registry=schema_registry)

    def produce(self, **kwargs):
        """
            Asynchronously sends message to Kafka by encoding with specified or default avro schema.
            :param str topic: topic name
            :param object value: An object to serialize
            :param str value_schema: Avro schema for value
            :param str key: An string to use as key
            Plus any other parameters accepted by confluent_kafka.Producer.produce
            :raises SerializerError: On serialization failure
            :raises BufferError: If producer queue is full.
            :raises KafkaException: For other produce failures.
        """
        # get schemas from  kwargs if defined
        value_schema = kwargs.pop('value_schema', self._value_schema)
        topic = kwargs.pop('topic', None)
        if not topic:
            raise ClientError("Topic name not specified.")
        value = kwargs.pop('value', None)
        key = kwargs.pop('key', None)

        if type(key) != str:
            raise ValueSerializerError("Messege key should be a str")

        if value is not None:
            if value_schema:
                value = self._serializer.encode_record_with_schema(
                    topic, value_schema, value)
            else:
                raise ValueSerializerError("Avro schema required for values")

        self.default_producer.produce(topic, value, key)