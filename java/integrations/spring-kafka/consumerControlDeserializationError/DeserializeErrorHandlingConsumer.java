package consumers.consumerControlDeserializationError;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class DeserializeErrorHandlingConsumer {

	public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
	public static final String GROUP_ID = "group";
	public static final String TOPIC = "avro_topic";

	private static Properties getConsumerProperties() {
		final Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Error Handling Deserializer to instance the proper delegates.
		// This will try to deserialize key and value with confluent avro (with schema
		// registry)
		// If the key or the value is not in avro it will return bytes in that part of
		// the message
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
		properties.put(ErrorHandlingDeserializer2.KEY_DESERIALIZER_CLASS, AVRO_DESERIALIZER);
		properties.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, AVRO_DESERIALIZER);
		return properties;
	}

	public static GenericRecord tryConvertToGenericRecord(ConsumerRecord<GenericRecord, GenericRecord> record,
			boolean key) {
		GenericRecord r = null;
		try {
			r = key ? record.key() : record.value();
		} catch (Exception e) {
			String errorPart = key ? "key" : "value";
			System.err.printf("CONTROLED ERROR : %s probably is encoded in avro but with a primitive type.\n",
					errorPart, errorPart);
			System.err.println("Probalby your schema is like schema = {  \"type\" : \"string\" }\n"
					+ "and connot be converted to GenericRecord\n");
		}
		return r;
	}

	public static void main(String[] args) {
		final KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(getConsumerProperties());
		consumer.subscribe(Collections.singleton(TOPIC));
		ConsumerRecords<GenericRecord, GenericRecord> consumerRecords;
		Iterator<ConsumerRecord<GenericRecord, GenericRecord>> iterator;
		ConsumerRecord<GenericRecord, GenericRecord> record;
		GenericRecord key, value;
		String keyType, valueType;
		try {
			while (true) {
				consumerRecords = consumer.poll(Duration.ofMillis(1000));
				iterator = consumerRecords.iterator();
				while (iterator.hasNext()) {
					record = iterator.next();
					key = tryConvertToGenericRecord(record, true);
					value = tryConvertToGenericRecord(record, false);
					keyType = key != null ? key.getClass().getName() : "null";
					valueType = value != null ? value.getClass().getName() : "null";
					System.out.printf("Consumer Record:(%s,%s) (%s, %s)\n", keyType, valueType, record.key(),
							record.value());
				}
				consumer.commitAsync();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
			System.out.println("DONE");
		}

	}
}
