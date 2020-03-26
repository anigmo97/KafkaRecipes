package common.use.cases;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class ConsumeMessagesFromCertainPoint {

	public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
	public static final String GROUP_ID = "group";
	public static final String TOPIC = "raw";

	private static Properties getConsumerProperties() {
		final Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AVRO_DESERIALIZER);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);

		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		return properties;
	}

	private static Properties getAdminClientProperties() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", KAFKA_BROKERS);
		properties.setProperty("client.id", "producerAdmin");
		properties.setProperty("metadata.max.age.ms", "3000");
		return properties;
	}

	/**
	 * Example of how, given a consumer and a list of topics, We obtain a Set of
	 * TopicPartition of those topics
	 */
	private static Set<TopicPartition> getListPartitionsOfConsumer(KafkaConsumer<String, GenericRecord> consumer,
			String[] topics) {

		Map<String, List<PartitionInfo>> partitionsInfo = consumer.listTopics();
		Set<TopicPartition> partitions = new HashSet<>();
		for (String topic : topics) {
			for (PartitionInfo p : partitionsInfo.get(topic)) {
				partitions.add(new TopicPartition(p.topic(), p.partition()));
			}
		}

		return partitions;
	}

	/**
	 * Example of how, given a consumer and a topic, We obtain a Set of
	 * TopicPartition of those topics
	 */
	public static List<TopicPartition> getPartitionsOfTopic(String topic,
			KafkaConsumer<String, GenericRecord> consumer) {
		// get info of all partitions of a topic
		List<PartitionInfo> partitionsInfo = consumer.partitionsFor(topic);

		// create TopicPartition set/list
		List<TopicPartition> partitions = new ArrayList<>();
		for (PartitionInfo p : partitionsInfo) {
			partitions.add(new TopicPartition(p.topic(), p.partition()));
		}
		return partitions;
	}

	/**
	 * We create a HashMap that relates a Partition with the start time when we will
	 * start reading messages. The offset will change on each partition for the same
	 * timestamp.
	 */

	private static Map<TopicPartition, OffsetAndTimestamp> getOffsetMapGivenTimestamp(List<TopicPartition> partitions,
			KafkaConsumer<String, GenericRecord> consumer, DateTime timeToStartReadMessagesFrom) {

		Map<TopicPartition, Long> timestampsPerPartitionMap = new HashMap<>();
		long longToStartReadMessagesFrom = timeToStartReadMessagesFrom.getMillis();

		for (TopicPartition tp : partitions) {
			timestampsPerPartitionMap.put(tp, longToStartReadMessagesFrom);
		}
		return consumer.offsetsForTimes(timestampsPerPartitionMap);

	}

	/**
	 * We create a HashMap that relates a Partition with the offset when we will
	 * start reading messages. In this case is the same for all partitions.
	 */
	private Map<TopicPartition, Long> getOffsetMapGivenOffset(List<TopicPartition> partitions,
			KafkaConsumer<String, GenericRecord> consumer, long offset) {
		Map<TopicPartition, Long> OffsetPerPartition = new HashMap<>();
		for (TopicPartition tp : partitions) {
			OffsetPerPartition.put(tp, offset);
		}
		return OffsetPerPartition;

	}

	public static void main(String[] args) {

		KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(
				getConsumerProperties());

		String topic = "avro_topic";

		// get partitions of topic
		List<TopicPartition> partitions = getPartitionsOfTopic(topic, consumer);

		// We assign all partitions (Consumer will read from all partitions)
		/*
		 * For reading from multiple topics we can do a for with unsuscribe and assign
		 */
		consumer.assign(partitions);

		// in this case we want to read messages from a specific timestamp
		DateTime timeToStartReadMessagesFrom = new DateTime(2020, 3, 26, 10, 03, 0);
		Map<TopicPartition, OffsetAndTimestamp> offsetByPartition = getOffsetMapGivenTimestamp(partitions, consumer,
				timeToStartReadMessagesFrom);

		for (TopicPartition tp : partitions) {
			consumer.seek(tp, offsetByPartition.get(tp).offset());
		}

		ConsumerRecords<String, GenericRecord> consumerRecords;
		while (true) {
			consumerRecords = consumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, GenericRecord> r : consumerRecords.records(topic)) {
				System.out.println(r.value());
			}
			// do something
			break;
		}
		consumer.close();
		System.out.println("DONE");

	}
}
