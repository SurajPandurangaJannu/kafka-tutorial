package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class SimpleKafkaConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SimpleKafkaConsumerApplication.class, args);
    }

    private static final String TOPIC = "lession-3";
    private static final String TOPIC_CG = "lession-3-cg";

    // 1. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 2. Create a kafka Template for Producer, Use the default one
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 3. Send messages to TOPIC
    @Scheduled(fixedRate = 10000)
    public void publishMessagesToTopic() {
        kafkaTemplate.send(TOPIC, LocalDateTime.now().toString())
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage());
                    } else {
                        log.info("Message Published successfully");
                    }
                }));
    }

    // 4. Consume messages from TOPIC
    @KafkaListener(topics = TOPIC, groupId = TOPIC_CG)
    public void consumeMessagesFromSimpleTopic(String message) {
        log.info("Message {} received successfully", message);
    }
}