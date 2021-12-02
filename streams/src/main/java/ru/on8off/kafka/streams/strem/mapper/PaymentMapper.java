package ru.on8off.kafka.streams.strem.mapper;

import org.apache.kafka.streams.kstream.ValueMapper;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.model.avro.EventTypeAvro;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;


public class PaymentMapper implements ValueMapper<PaymentEventAvro,  Payment> {

    @Override
    public Payment apply(PaymentEventAvro value) {
        var payment = new Payment();
        if(value.getEventType() == EventTypeAvro.PaymentPageOpened) {
            payment.setPaymentPageOpenedTime(value.getTimestamp());
        } else if (value.getEventType() == EventTypeAvro.PaymentTypeSelected) {
            payment.setPaymentTypeSelectedTime(value.getTimestamp());
        } else if (value.getEventType() == EventTypeAvro.PaymentDataFilled) {
            payment.setPaymentDataFilledTime(value.getTimestamp());
        } else if(value.getEventType() == EventTypeAvro.PaymentSucceed) {
            payment.setPaymentSucceedTime(value.getTimestamp());
        } else if(value.getEventType() == EventTypeAvro.PaymentFailed) {
            payment.setPaymentFailedTime(value.getTimestamp());
        }
        return payment;
    }
}
