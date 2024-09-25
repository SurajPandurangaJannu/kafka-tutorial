package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class ComsumeFromMultipleTopicsApplication {

    public static void main(String[] args) {
        SpringApplication.run(ComsumeFromMultipleTopicsApplication.class, args);
    }

    private static final String TOPIC_1 = "lession-11-1";
    private static final String TOPIC_2 = "lession-11-2";

    private static final String TOPIC_CG = "lession-11-cg";

    // 1. Create 2 topics
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC_1, 3, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic(TOPIC_2, 3, (short) 1);
    }

    // 2. Create kafka template for producer
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 2. Send messages to both topics
    @Scheduled(fixedRate = 10000)
    public void publishMessageToSimpleTask() {
        kafkaTemplate.send(TOPIC_1, LocalDateTime.now().toString())
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage());
                    } else {
                        log.info("Message Published successfully");
                    }
                }));

        kafkaTemplate.send(TOPIC_2, LocalDateTime.now().toString())
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage());
                    } else {
                        log.info("Message Published successfully");
                    }
                }));
    }

    // 3. consume messages from both first topic and second topic
    @KafkaListener(topics = {TOPIC_1, TOPIC_2}, groupId = TOPIC_CG)
    public void consumeMessagesFromBothTopic(ConsumerRecord<String, String> message) {
        log.info("Message received from Topic :: {} and Value is {}", message.topic(), message.value());
    }

}