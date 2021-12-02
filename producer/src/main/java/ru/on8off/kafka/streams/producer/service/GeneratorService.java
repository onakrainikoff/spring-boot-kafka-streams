package ru.on8off.kafka.streams.producer.service;

import org.springframework.stereotype.Service;
import ru.on8off.kafka.streams.model.avro.EventTypeAvro;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class GeneratorService {
    private final Random random = new Random();

    public List<PaymentEventAvro> generateChainForAvro() {
        var timeStamp = System.currentTimeMillis();
        var customerId = (long) random.nextInt(100);
        var chain = new ArrayList<PaymentEventAvro>();
        chain.add(new PaymentEventAvro(timeStamp, customerId, EventTypeAvro.PaymentPageOpened));
        chain.add(new PaymentEventAvro(timeStamp, customerId, EventTypeAvro.PaymentTypeSelected));
        chain.add(new PaymentEventAvro(timeStamp, customerId, EventTypeAvro.PaymentDataFilled));
        var factor = random.nextInt(100);
        if(factor < 45) {
            chain.add(new PaymentEventAvro(timeStamp, customerId, EventTypeAvro.PaymentSucceed));
        } else if(factor < 80) {
            chain.add(new PaymentEventAvro(timeStamp, customerId, EventTypeAvro.PaymentFailed));
        }
        return chain;
    }
}
