package ru.on8off.kafka.streams.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;

import java.util.Objects;
import java.util.Random;

@Service
@Slf4j
public class ProducerService {
    @Value("${topic.avro}")
    private String topicAvro;
    @Autowired
    private KafkaTemplate<Long, PaymentEventAvro> avroPaymentEventKafkaTemplate;
    @Autowired
    private GeneratorService generator;
    private Random random = new Random();

    public void produce(Integer count, Integer pause) throws InterruptedException {
        count = Objects.requireNonNullElse(count,1);
        pause = Objects.requireNonNullElse(count, 1000 + random.nextInt(5) * 1000);
        for (int i = 0; i < count; i++) {
            var events = generator.generateChainForAvro();
            for (PaymentEventAvro event : events) {
                event.setTimestamp(System.currentTimeMillis());
                var result  = avroPaymentEventKafkaTemplate.send(topicAvro, null, event.getCustomerId(), event);
                result.addCallback(new ProducerListener(i, topicAvro, event));
                log.info(">>> sleep {} ms", pause);
                Thread.sleep(pause);
            }
        }
    }

    private class ProducerListener implements ListenableFutureCallback<SendResult<Long, PaymentEventAvro>> {
        private int n;
        private String topic;
        private PaymentEventAvro event;

        public ProducerListener(int n, String topic, PaymentEventAvro event) {
            this.n = n;
            this.topic = topic;
            this.event = event;
        }

        @Override
        public void onSuccess(SendResult<Long, PaymentEventAvro> result) {
            log.info(">>> produceAvro {} succeed: topic={}, key={}, eventType={}", n, topicAvro, event.getCustomerId(), event.getEventType());
        }
        @Override
        public void onFailure(Throwable ex) {
            log.info(">>> produceAvro {} onFailure: {}", n, ex.getMessage());
        }
    }


}
