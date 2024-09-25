package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;

@SpringBootApplication
@EnableScheduling // 1. First step to enable scheduling
@Slf4j
public class SimpleKafkaProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SimpleKafkaProducerApplication.class, args);
    }

    private static final String TOPIC = "lession-2";

    // 2. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 3. Create a Kafka Template for Producer. Thanks to Spring Booot we have the default template available already
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 4. Send messages to TOPIC
    @Scheduled(fixedRate = 5000)
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
}