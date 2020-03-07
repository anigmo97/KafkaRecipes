from confluent_kafka.avro import AvroConsumer

class StringKeyAvroConsumer(AvroConsumer):
    def __init__(self, config, schema_registry=None, reader_key_schema=None, reader_value_schema=None):

        super(StringKeyAvroConsumer,self).__init__(config, schema_registry=schema_registry,
                reader_key_schema=reader_key_schema, reader_value_schema=reader_value_schema)


    def poll(self,timeout=None):
        """ 
        This is an overriden method from AvroConsumer class.\n
        This hadles message deserialization using avro schema for the value only.

        @:param timeout
        @:return message object with deserialized key and value as dict objects
        """

        if timeout is None:
            timeout = -1
        message = super(AvroConsumer,self).poll(timeout)
        if message is None:
            return None
        if not message.value() and not message.key():
            return message

        if not message.error():
            if message.value() is not None:
                decoded_value = self._serializer.decode_message(message.value())
                message.set_value(decoded_value)

            if message.key():
                decoded_key = message.key().decode("utf-8")
                message.set_key(decoded_key)
            #do no
        return message



