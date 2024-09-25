package com.surajjannu.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class KafkaTopicCreationApplication {

    private static final String TOPIC_1 = "lession-1-1";
    private static final String TOPIC_2 = "lession-1-2";

    public static void main(String[] args) {
        SpringApplication.run(KafkaTopicCreationApplication.class, args);
    }

    // 1. First Method of creating topic
    // New Topic can be created using NewTopic class
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC_1, 3, (short) 1);
    }

    // 1. Second Method of creating topic
    // Topic can also be built using TopicBuilder class
    @Bean
    public NewTopic topic2() {
        return TopicBuilder
                .name(TOPIC_2)
                .partitions(3)
                .replicas(1)
                .build();
    }

}