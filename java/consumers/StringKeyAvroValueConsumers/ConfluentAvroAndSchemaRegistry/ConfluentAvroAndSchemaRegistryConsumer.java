package consumers.stringKeyAvroValueConsumers;

import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class ConfluentAvroAndSchemaRegistryConsumer {

	public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
	public static final String GROUP_ID = "group";
	public static final String TOPIC = "raw";

	private static Properties getConsumerProperties() {
		final Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AVRO_DESERIALIZER);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		return properties;
	}

	public static void main(String[] args) {
		final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(getConsumerProperties());
		consumer.subscribe(Collections.singleton(TOPIC));
		ConsumerRecords<String, GenericRecord> consumerRecords;
		try {
			while (true) {
				consumerRecords = consumer.poll(1000);

				consumerRecords.forEach(record -> {
					System.out.printf("Consumer Record:(%s, %s)\n", record.key(), record.value());
				});

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

