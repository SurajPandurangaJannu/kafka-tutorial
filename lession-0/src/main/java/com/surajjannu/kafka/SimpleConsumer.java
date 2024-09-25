package com.surajjannu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    private static final String TOPIC = "lession-0";
    private static final String TOPIC_CG = "lession-0-cg";

    public static void main(String[] args) {
        final Map<String, Object> producerConfig = producerConfig();
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(producerConfig)) {
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Key :: {}  -> Value :: {}", consumerRecord.key(), consumerRecord.value());
                    log.info("Partition :: {} -> Offset :: {} -> Timestamp :: {}", consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp());
                }
            }
        }
    }

    private static Map<String, Object> producerConfig() {
        final HashMap<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, TOPIC_CG);
        return consumerConfig;
    }
}