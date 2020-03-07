# KafkaRecipes
This is a bunch of useful kafka examples

In the examples of consumers and Producers, the read and written messages have this schema:

> Key : string
> value: {
>     id:int,
>     date:int,
>     info:string
> }

Or the equivalent avro schema:
> {
>     "type": "record",
>     "namespace": "example.avro",
>     "name": "test_record",
>     "fields": [
>         {"name": "id", "type": "int"},
>         {"name": "date", "type": ["int", "null"]},
>         {"name": "info", "type": "string"}
>     ]
> }

The serializations used in the examples are json, standard avro and confluent avro.
The difference between confluent avro and standard avro is that in order to use schema registry, confluent avro prepends the message with four bytes that indicates which schema should be used.
This causes some integrations to only read one of the two.
