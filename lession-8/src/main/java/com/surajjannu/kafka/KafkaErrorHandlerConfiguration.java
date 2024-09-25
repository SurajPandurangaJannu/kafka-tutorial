package com.surajjannu.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;

@Configuration
@Slf4j
public class KafkaErrorHandlerConfiguration {

    // 4. In case of message processing goes wrong and some exception occurs we need to callback to handle such scenarios.
    // So configure KafkaListenerErrorHandler bean for such scenarios
    @Bean
    public KafkaListenerErrorHandler kafkaListenerErrorHandler() {
        return new KafkaListenerErrorHandler() {
            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
                log.error("Exception occured with message :: " + exception.getLocalizedMessage());
                log.error("Exception occured with exception class :: " + exception.getClass().getName());
                log.error("Exception occured with message payload :: " + message.getPayload());
                return message;
            }
        };
    }

}
