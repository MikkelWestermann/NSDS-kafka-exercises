package it.polimi.middleware.kafka.ex1;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Consumer2 {
  private static final String defaultGroupId = "groupB";
  private static final String defaultTopic = "topicA";

  private static final String serverAddr = "localhost:9092";
  private static final boolean autoCommit = true;
  private static final int autoCommitIntervalMs = 15000;

  // Default is "latest": try "earliest" instead
  private static final String offsetResetStrategy = "latest";

  private static final String newTopic = "topicB";

  public static void main(String[] args) {
    // If there are arguments, use the first as group and the second as topic.
    // Otherwise, use default group and topic.
    String groupId = args.length >= 1 ? args[0] : defaultGroupId;
    String topic = args.length >= 2 ? args[1] : defaultTopic;

    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    while (true) {
      final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
      for (final ConsumerRecord<String, String> record : records) {
        String value = record.value();
        value = value.replaceAll("[A-Z]", "");

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        final Random r = new Random();
        final String key = "Key" + r.nextInt(1000);

        final ProducerRecord<String, String> newRecord = new ProducerRecord<>(newTopic, key, value);
        final Future<RecordMetadata> future = producer.send(newRecord);
        producer.close();
      }
    }
  }
}
