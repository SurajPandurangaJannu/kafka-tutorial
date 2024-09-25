package com.surajjannu.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SimpleProducer {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class.getName());

    private static final String TOPIC = "lession-0";

    public static void main(String[] args) {
        final Map<String, Object> producerConfig = producerConfig();
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig)) {
            int i = 0;
            while (true) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "Key-" + i, "Value-" + i);
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception throwable) {
                        if (throwable != null) {
                            log.error(throwable.getMessage());
                        } else {
                            log.info("Message Published to Successfully --> Topic :: {} , Partition :: {},Offset :: {} ,Timestamp :: {}",
                                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                        }
                    }
                });
                i++;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private static Map<String, Object> producerConfig() {
        final HashMap<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerConfig;
    }
}