package com.sample.event.config;

import com.sample.event.annotation.EventType;
import com.sample.event.consumer.InstrumentEventListener;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

@EnableKafka
@Configuration
public class KafkaEventConfig {
    private final Logger log = LoggerFactory.getLogger(KafkaEventConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;


    @Autowired
    private InstrumentEventListener instrumentEventListener;



    /**
     *  <a href="https://www.javacodegeeks.com/2018/05/spring-apache-kafka-tutorial.html">Reference</a>
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        // Creating a Map of string-object pairs
        Map<String, Object> config = new HashMap<>();

        // Adding the Configuration
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @KafkaListener(topics = "instrument.topic", groupId = "groupId-1")
    public void consume(ConsumerRecord<String, String> record) throws InvocationTargetException, IllegalAccessException {
        System.out.println("record = " + record);
        String message = record.value();
        Map<String, String> headers = toSingleValueMap(record.headers());

        String eventType = headers.get("EVENT_TYPE"); // Assuming "EVENT_TYPE" header
        if (eventType != null) {
            for (Method method : instrumentEventListener.getClass().getDeclaredMethods()) {
                EventType eventTypeAnn = method.getAnnotation(EventType.class);
                if (Arrays.stream(eventTypeAnn.value()).anyMatch(x -> x.equalsIgnoreCase(eventType))) {
                    method.invoke(this, message, headers);
                }
            }
        } else {
            log.warn("No event type found in the header for the message");
        }

    }

    private Map<String, String> toSingleValueMap(Headers headers) {
        Map<String, String> headersMap = new HashMap<>(); // C
        for (Header header : headers) {
            headersMap.put(header.key(), new String(header.value())); // Convert header value to String
        }
        return headersMap;
    }

}
