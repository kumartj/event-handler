package com.sample.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

@SpringBootTest
@TestPropertySource(
        properties = {
                "spring.kafka.consumer.auto-offset-reset=earliest",
        }
)
public class EventApplicationIntegrationTest {

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }


    @ClassRule
    public static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.3")
    );

    @Test
    public void testConsumeInstrumentCreateEvent() throws Exception {

        Map<String, Object> properties = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record1 = new ProducerRecord<>("instrument.topic", 0, "key",  "CREATE_EVENT", List.of(header("EVENT_TYPE", "INSTRUMENT_CREATE")));
        kafkaProducer.send(record1);
        ProducerRecord<String, String> record2 = new ProducerRecord<>("instrument.topic", 0, "key",  "UPDATE_EVENT", List.of(header("EVENT_TYPE", "INSTRUMENT_UPDATE")));
        kafkaProducer.send(record2);

        Thread.sleep(50000);
    }

    private static Header header(String key, String value) {
        return  new Header() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public byte[] value() {
                return value.getBytes();
            }
        };
    }
}
