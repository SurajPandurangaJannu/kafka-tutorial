package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
public class ForwardListenerResultToNewTopicApplication {

    public static void main(String[] args) {
        SpringApplication.run(ForwardListenerResultToNewTopicApplication.class, args);
    }

    private static final String TOPIC_1 = "lession-10-1";
    private static final String TOPIC_2 = "lession-10-2";

    private static final String TOPIC_1_CG = "lession-10-1-cg";
    private static final String TOPIC_2_CG = "lession-10-2-cg";

    // 1. Create 2 topics
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC_1, 3, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic(TOPIC_2, 3, (short) 1);
    }

    // 2. Create a Kafka Template for producer
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 3. Send messages to first topic
    @Scheduled(fixedRate = 10000)
    public void publishMessageToTopic() {
        kafkaTemplate.send(TOPIC_1, LocalDateTime.now().toString())
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage());
                    } else {
                        log.info("Message Published successfully");
                    }
                }));
    }

    // 4. consume messages in first topic and send it to second topic
    @KafkaListener(topics = TOPIC_1, groupId = TOPIC_1_CG)
    @SendTo(value = TOPIC_2)
    public String consumeMessagesFromFirstTopic(String message) {
        log.info(TOPIC_1 + " message :: " + message);
        return message;
    }

    // 5. Consume messages from second topic
    @KafkaListener(topics = TOPIC_2, groupId = TOPIC_2_CG)
    public void consumeMessagesFromSecondTopic(String message) {
        log.info(TOPIC_2 + " message :: " + message);
    }
}