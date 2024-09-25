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
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.List;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class MessageBatchingApplication {

    public static void main(String[] args) {
        SpringApplication.run(MessageBatchingApplication.class, args);
    }


    private static final String TOPIC = "lession-9";
    private static final String TOPIC_CG = "lession-9-cg";

    // 1. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 2. Create a Kafka Template for producer
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 3. Publish messages into topic
    @Scheduled(fixedRate = 10000)
    public void publishMessageToTopic() {
        for (int i = 0; i < 100; i++) {
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

    // 4. Consume messages from topic in batches, you can also configure maximum number of batches as well
    @KafkaListener(topics = TOPIC, groupId = TOPIC_CG, batch = "true", properties = "max.poll.records=10")
    public void consumeMessagesFromSimpleTopic(List<String> messages) {
        log.info("Message count is {} ", messages.size());
    }

}