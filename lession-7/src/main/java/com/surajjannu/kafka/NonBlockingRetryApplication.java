package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class NonBlockingRetryApplication {

    public static void main(String[] args) {
        SpringApplication.run(NonBlockingRetryApplication.class, args);
    }

    private static final String TOPIC = "lession-7";
    private static final String TOPIC_CG = "lession-7-cg";

    // 1. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 2. Create a Kafka Template for Producer
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 3. Publish message into topic
    @Scheduled(fixedRate = 10000)
    public void publishMessageToTopic() {
        kafkaTemplate.send(TOPIC, LocalDateTime.now().toString())
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage());
                    } else {
                        log.info("Message Published successfully");
                    }
                }));
    }

    // 5. Incase if the message processing goes wrong configure re-try
    // retry should be non blocking that means the other messages should not be affected
    // Retry can be configured with some delay
    // We can also configure the number of times the messages should be retried
    @RetryableTopic(attempts = "5",
            kafkaTemplate = "kafkaTemplate",
            backoff = @Backoff(delay = 2_000, maxDelay = 10_000, multiplier = 2)
    )
    // 4. Consume messages from topic
    @KafkaListener(topics = TOPIC, groupId = TOPIC_CG)
    public void consumeMessagesFromTopic(ConsumerRecord<String, String> message) {
        // Some logic
        throw new RuntimeException("Invalid Message received at partiton " + message.partition() + " and offset " + message.offset());
        // some logic
    }

    // 6. Upon all retry attempts even if the message failed to process the messages should be send to dead-letter-topic
    @DltHandler
    public void deadLetterTopicHandler(ConsumerRecord<String, String> message) {
        log.info("________________________________________________________");
        log.info("Message received successfully from topic {} ", message.topic());
        log.info("Key :: {}  -> Value :: {}", message.key(), message.value());
        log.info("Partition :: {} -> Offset :: {} -> Timestamp :: {}", message.partition(), message.offset(), message.timestamp());
        log.info("________________________________________________________");
    }

}