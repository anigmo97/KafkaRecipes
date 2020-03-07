package producers.stringKeyAvroValueProducers;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class ConfluentAvroAndSchemaRegistryProducer {

	public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";
	public static final String TOPIC = "raw";

	private static Properties getProducerProperties() {
		final Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER);

		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		return properties;
	}

	public static void main(String[] args) {
		final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(getProducerProperties());
		int messagesToSend = 10;

		String valueSchemaString = "{\"type\": \"record\",\"namespace\": \"example.avro\",\"name\": \"test_record\","
				+ "\"fields\":[" + "{\"name\": \"id\",\"type\": \"int\"},"
				+ "{\"name\": \"date\",\"type\": [\"int\", \"null\"]}," + "{\"name\": \"info\",\"type\": \"string\"}"
				+ "]}}";
		Schema avroValueSchema = new Schema.Parser().parse(valueSchemaString);
		GenericRecord value;
		String key;
		ProducerRecord<String, GenericRecord> record;
		RecordMetadata metadata;
		try {
			for (int i = 0; i < messagesToSend; i++) {
				value = new GenericData.Record(avroValueSchema);
				value.put("id", i);
				value.put("date", (int) (2 + Math.pow(i, 2)));
				value.put("info", "sensor_" + Integer.toString(i));
				key = "message_key_" + Integer.toString(i);
				record = new ProducerRecord<String, GenericRecord>(TOPIC, key, value);

				metadata = producer.send(record).get();

				System.out.printf("sent record key=%s value=%s meta partition=%s, offset=%s \n", record.key(),
						record.value(), metadata.partition(), metadata.offset());
				Thread.sleep(1000);

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
			System.out.println("DONE");
		}

	}

}

