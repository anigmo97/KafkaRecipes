package producers.stringKeyAvroValueProducers;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class StandardAvroProducer {

	public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
	public static final String TOPIC = "raw";

	private static Properties getProducerProperties() {
		final Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		return properties;
	}

	public static void main(String[] args) {
		final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(getProducerProperties());
		int messagesToSend = 10;

		String valueSchemaString = "{\"type\": \"record\",\"namespace\": \"example.avro\",\"name\": \"test_record\","
				+ "\"fields\":[" + "{\"name\": \"id\",\"type\": \"int\"},"
				+ "{\"name\": \"date\",\"type\": [\"int\", \"null\"]}," + "{\"name\": \"info\",\"type\": \"string\"}"
				+ "]}}";
		Schema avroValueSchema = new Schema.Parser().parse(valueSchemaString);
		GenericRecord value;
		byte[] serializedValue;
		String key;
		ProducerRecord<String, byte[]> record;
		RecordMetadata metadata;
		GenericRecordBuilder builder = new GenericRecordBuilder(avroValueSchema);
		try {
			for (int i = 0; i < messagesToSend; i++) {
				Record valueAvroRecord = builder.set("id", i).set("date", (int) (2 + Math.pow(i, 2)))
						.set("info", "sensor_" + Integer.toString(i)).build();

				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				BinaryEncoder encoder = new EncoderFactory().get().binaryEncoder(outputStream, null);
				DatumWriter<Record> datumWriter = new GenericDatumWriter<Record>(avroValueSchema);
				datumWriter.write(valueAvroRecord, encoder);
				encoder.flush();
				serializedValue = outputStream.toByteArray();
				key = "message_key_" + Integer.toString(i);
				// trick -> byte[] serializer does nothing
				record = new ProducerRecord<String, byte[]>(TOPIC, key, serializedValue);

				metadata = producer.send(record).get();
				outputStream.close();

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

