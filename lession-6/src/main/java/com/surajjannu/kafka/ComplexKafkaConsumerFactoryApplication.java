package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.HashMap;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class ComplexKafkaConsumerFactoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(ComplexKafkaConsumerFactoryApplication.class, args);
    }


    private static final String TOPIC = "lession-6";
    private static final String TOPIC_CG = "lession-6-cg";

    // 1. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 2. Create a kafka template for producer
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 3. publish message into topic
    @Scheduled(fixedRate = 10000)
    public void publishMessageToSimpleTask() {
        kafkaTemplate.send(TOPIC, LocalDateTime.now().toString())
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage());
                    } else {
                        log.info("Message Published successfully");
                    }
                }));
    }

    // 5. Use the ConcurrentKafkaListenerContainerFactory bean while consuming the messages
    @KafkaListener(topics = TOPIC, groupId = TOPIC_CG, containerFactory = "kafkaComplexConsumerFactory", concurrency = "3")
    public void consumeMessagesFromSimpleTopic(ConsumerRecord<String, String> message) {
        log.info("Message received successfully");
        log.info("Key :: {}  -> Value :: {}", message.key(), message.value());
        log.info("Partition :: {} -> Offset :: {} -> Timestamp :: {}", message.partition(), message.offset(), message.timestamp());
    }

}