package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class ComplexKafkaConsumerApplication {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    @Autowired
    @Lazy
    private KafkaTemplate<String, ComplexData> complexDataKafkaTemplate;

    private static final String TOPIC = "lession-5";
    private static final String TOPIC_CG = "lession-5-cg";

    public static void main(String[] args) {
        SpringApplication.run(ComplexKafkaConsumerApplication.class, args);
    }

    // 2. Create a Topic
    @Bean
    public NewTopic topic1() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    // 3. Create a Custom Kafka Template for Producer
    @Bean
    public KafkaTemplate<String, ComplexData> complexDataKafkaTemplate() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        final DefaultKafkaProducerFactory<String, ComplexData> producerFactory =
                new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new JsonSerializer<>());
        return new KafkaTemplate<>(producerFactory);
    }

    // 4. publish the message into TOPIC
    @Scheduled(fixedRate = 10000)
    public void publishMessageToTopic() {
        final ComplexData complexData = new ComplexData();
        complexData.setStringValue(generateRandomString(10));
        complexData.setIntValue(new Random().nextInt());
        complexData.setDoubleValue(new Random().nextDouble());
        complexData.setBoolValue(new Random().nextBoolean());

        final ProducerRecord<String, ComplexData> producerRecord =
                new ProducerRecord<>(TOPIC, LocalDateTime.now().toString(), complexData);

        complexDataKafkaTemplate.send(producerRecord)
                .whenComplete(((result, throwable) -> {
                    if (throwable != null) {
                        log.info("*****************************************************");
                        log.error(throwable.getMessage());
                        log.info("*****************************************************");
                    } else {
                        RecordMetadata recordMetadata = result.getRecordMetadata();
                        log.info("=====================================================");
                        log.info("Published message with Key :: {} and value :: {}", producerRecord.key(), producerRecord.value());
                        log.info("Message Published to Successfully --> Topic :: {} , Partition :: {},Offset :: {} ,Timestamp :: {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                        log.info("=====================================================");
                    }
                }));
    }

    public static String generateRandomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }
        return sb.toString();
    }

    // 5. Consume Message as a Consumer Record to get more details
    @KafkaListener(topics = TOPIC, groupId = TOPIC_CG)
    public void consumeMessagesFromTopic(ConsumerRecord<String, ComplexData> message) {
        log.info("Message received successfully");
        log.info("Key :: {}  -> Value :: {}", message.key(), message.value());
        log.info("Partition :: {} -> Offset :: {} -> Timestamp :: {}", message.partition(), message.offset(), message.timestamp());
    }
}