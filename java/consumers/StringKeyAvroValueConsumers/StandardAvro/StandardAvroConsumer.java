package consumers.stringKeyAvroValueConsumers;

import java.time.Duration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class StandardAvroConsumer {

	public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
	public static final String GROUP_ID = "group";
	public static final String TOPIC = "raw";

	private static Properties getConsumerProperties() {
		final Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		return properties;
	}

	public static void main(String[] args) {
		final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(getConsumerProperties());
		consumer.subscribe(Collections.singleton(TOPIC));
		ConsumerRecords<String, byte[]> consumerRecords;
		//this is the schema of the sent records from our producers
		String valueSchemaString = "{\"type\": \"record\",\"namespace\": \"example.avro\",\"name\": \"test_record\","
				+ "\"fields\":[" + "{\"name\": \"id\",\"type\": \"int\"},"
				+ "{\"name\": \"date\",\"type\": [\"int\", \"null\"]}," + "{\"name\": \"info\",\"type\": \"string\"}"
				+ "]}}";
		Schema avroValueSchema = new Schema.Parser().parse(valueSchemaString);
		SpecificDatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(avroValueSchema);
		try {
			while (true) {
				consumerRecords = consumer.poll(Duration.ofMillis(1000));

				consumerRecords.forEach(record -> {
					ByteArrayInputStream inputStream = new ByteArrayInputStream(record.value());
					BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
					GenericRecord deserializedValue = null;
					try {
						deserializedValue = datumReader.read(null, binaryDecoder);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.printf("Consumer Record:(%s, %s)\n", record.key(), deserializedValue);
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

