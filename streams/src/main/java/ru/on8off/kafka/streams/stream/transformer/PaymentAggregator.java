package ru.on8off.kafka.streams.stream.transformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Aggregator;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.model.avro.EventTypeAvro;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;

@Slf4j
public class PaymentAggregator implements Aggregator<Long, PaymentEventAvro, Payment> {
    @Override
    public Payment apply(Long key, PaymentEventAvro value, Payment aggregate) {
        log.info(">>> paymentAggregator key={} value={} aggregate={}", key, value, aggregate);
        aggregate.setCustomerId(key);
        if(value.getEventType() == EventTypeAvro.PaymentPageOpened) {
            aggregate.setPaymentPageOpenedTime(value.getTimestamp());
        } else if (value.getEventType() == EventTypeAvro.PaymentTypeSelected) {
            aggregate.setPaymentTypeSelectedTime(value.getTimestamp());
        } else if (value.getEventType() == EventTypeAvro.PaymentDataFilled) {
            aggregate.setPaymentDataFilledTime(value.getTimestamp());
        } else if(value.getEventType() == EventTypeAvro.PaymentSucceed) {
            aggregate.setPaymentSucceedTime(value.getTimestamp());
        } else if(value.getEventType() == EventTypeAvro.PaymentFailed) {
            aggregate.setPaymentFailedTime(value.getTimestamp());
        }
        return aggregate;
    }
}
