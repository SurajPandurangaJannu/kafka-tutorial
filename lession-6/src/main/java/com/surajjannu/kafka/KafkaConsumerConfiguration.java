package com.surajjannu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;

@Configuration
public class KafkaConsumerConfiguration {
    // 4. You can override the consumer properties by creating the ConcurrentKafkaListenerContainerFactory bean
    @Bean("kafkaComplexConsumerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaComplexConsumerFactory() {
        final HashMap<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer());
        final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        containerFactory.setConsumerFactory(consumerFactory);
        return containerFactory;
    }

}
