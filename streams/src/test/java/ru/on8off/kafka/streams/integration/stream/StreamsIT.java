package ru.on8off.kafka.streams.integration.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import ru.on8off.kafka.streams.integration.configuration.TestKafkaConfiguration;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.model.avro.EventTypeAvro;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@Import(TestKafkaConfiguration.class)
public class StreamsIT {
    @Value("${topics.source}")
    private String paymentEventsAvroTopic;
    @Autowired
    KafkaTemplate<Long, PaymentEventAvro> avroPaymentEventKafkaTemplate;
    @Autowired
    KafkaConsumer<Long, Payment> paymentsSuccessKafkaConsumer;
    @Autowired
    KafkaConsumer<Long, Payment> paymentsAbandonedKafkaConsumer;
    @Autowired
    KafkaConsumer<Long, Payment> paymentsFailedKafkaConsumer;

    @Test
    public void testStreams() throws InterruptedException {
        generateSuccess().forEach( p ->{
            avroPaymentEventKafkaTemplate.send(paymentEventsAvroTopic, p.getCustomerId(), p);
        });
        generateFailed().forEach( p ->{
            avroPaymentEventKafkaTemplate.send(paymentEventsAvroTopic, p.getCustomerId(), p);
        });
        generateAbandoned().forEach( p ->{
            avroPaymentEventKafkaTemplate.send(paymentEventsAvroTopic, p.getCustomerId(), p);
        });

        var listPaymentsSuccess = consume(paymentsSuccessKafkaConsumer);
        var listPaymentsFailed = consume(paymentsFailedKafkaConsumer);
        var listPaymentsAbandoned = consume(paymentsAbandonedKafkaConsumer);

        Assertions.assertNotNull(listPaymentsSuccess);
        Assertions.assertNotNull(listPaymentsFailed);
        Assertions.assertNotNull(listPaymentsAbandoned);

        Assertions.assertEquals(1, listPaymentsSuccess.size());
        Assertions.assertEquals(1, listPaymentsFailed.size());
        Assertions.assertEquals(1, listPaymentsAbandoned.size());
    }

    private <T> List<T> consume(KafkaConsumer<Long, T> consumer) {
        var attempts = 10;
        var current = 0;
        List<T> result = null;
        while (result == null && current < attempts){
            var records = consumer.poll(Duration.ofSeconds(1));
            if(records != null && records.count() > 0) {
                result = new ArrayList<>();
                for (ConsumerRecord<Long, T> record : records) {
                    result.add(record.value());
                }
            }
            System.out.println(">>> attempt=" + current);
            current ++;
        }
        return  result;
    }

    private List<PaymentEventAvro> generateSuccess(){
        var time = System.currentTimeMillis();
        var id = 1L;
        return List.of(
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentPageOpened),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentTypeSelected),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentDataFilled),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentSucceed)
        );
    }

    private List<PaymentEventAvro> generateFailed(){
        var time = System.currentTimeMillis();
        var id = 2L;
        return List.of(
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentPageOpened),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentTypeSelected),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentDataFilled),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentFailed)
        );
    }

    private List<PaymentEventAvro> generateAbandoned(){
        var time = System.currentTimeMillis();
        var id = 3L;
        return List.of(
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentPageOpened),
                new PaymentEventAvro( time, id, EventTypeAvro.PaymentTypeSelected)
        );
    }

}
