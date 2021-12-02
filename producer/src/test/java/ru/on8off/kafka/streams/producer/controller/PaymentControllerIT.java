package ru.on8off.kafka.streams.producer.controller;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import ru.on8off.kafka.streams.model.avro.EventTypeAvro;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;
import ru.on8off.kafka.streams.producer.configuration.TestKafkaConfiguration;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestKafkaConfiguration.class)
class PaymentControllerIT {
    @Autowired
    private KafkaConsumer<Long, PaymentEventAvro> kafkaConsumer;
    @Autowired
    private TestRestTemplate restTemplate;
    @LocalServerPort
    private int port;
    private String host = "http://localhost:";

    @Test
    void generateNewPayment(){
        var response = restTemplate.getForEntity(host + port + "/generate?count=1&pause=10", String.class);
        Assertions.assertEquals(200, response.getStatusCodeValue());
        Assertions.assertEquals("OK", response.getBody());
        var result = KafkaTestUtils.getRecords(kafkaConsumer);
        Assertions.assertEquals(EventTypeAvro.PaymentPageOpened, result.iterator().next().value().getEventType());
    }

}