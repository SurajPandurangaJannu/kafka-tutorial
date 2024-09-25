package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.Backoff;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.HashMap;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class ErrorHandlingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ErrorHandlingApplication.class, args);
    }

    private static final String TOPIC = "lession-8";
    private static final String TOPIC_CG = "lession-8-cg";

    // 1. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 2. Create a kafka Template for Producer
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 3. publish message into topic
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


    // 5. Consume messages from topic
    @KafkaListener(topics = TOPIC, groupId = TOPIC_CG, errorHandler = "kafkaListenerErrorHandler")
    public void consumeMessagesFromSimpleTopic(ConsumerRecord<String, String> message) {
        // some logic
        throw new RuntimeException("Invalid Message received at partiton " + message.partition() + " and offset " + message.offset());
        // some logic
    }


}