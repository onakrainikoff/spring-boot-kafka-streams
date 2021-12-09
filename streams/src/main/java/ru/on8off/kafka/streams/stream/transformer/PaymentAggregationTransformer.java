package ru.on8off.kafka.streams.stream.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.on8off.kafka.streams.model.Payment;

import java.time.Duration;

public class PaymentAggregationTransformer implements Transformer<Long, Payment, KeyValue<Long, Payment>> {
    private String storeName;
    private Long aggregationWindowMs;
    private KeyValueStore<Long, Payment> store;
    private ProcessorContext context;

    public PaymentAggregationTransformer(String storeName, Long aggregationWindowMs) {
        this.storeName = storeName;
        this.aggregationWindowMs = aggregationWindowMs;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
        this.context.schedule(Duration.ofMillis(aggregationWindowMs), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public KeyValue<Long, Payment> transform(Long key, Payment value) {
        var payment = store.get(key);
        store.put(key, aggregate(key, value, payment));
        return null;
    }

    @Override
    public void close() {

    }

    private void punctuate(final long currentTimeMs){
        try (final KeyValueIterator<Long, Payment> iterator = store.all()) {
            while (iterator.hasNext()) {
                var keyValue = iterator.next();
                context.forward(keyValue.key, keyValue.value);
                store.delete(keyValue.key);
            }
        }
    }

    private Payment aggregate(Long key, Payment value, Payment aggregate) {
        if(aggregate == null){
            aggregate = new Payment();
        }
        aggregate.setCustomerId(key);

        if(value.getPaymentPageOpenedTime() != null) {
            aggregate.setPaymentPageOpenedTime(value.getPaymentPageOpenedTime());
        } else if (value.getPaymentTypeSelectedTime()!=null) {
            aggregate.setPaymentTypeSelectedTime(value.getPaymentTypeSelectedTime());
        } else if (value.getPaymentDataFilledTime() != null) {
            aggregate.setPaymentDataFilledTime(value.getPaymentDataFilledTime());
        } else if(value.getPaymentSucceedTime() != null) {
            aggregate.setPaymentSucceedTime(value.getPaymentSucceedTime());
        } else if(value.getPaymentFailedTime() != null) {
            aggregate.setPaymentFailedTime(value.getPaymentFailedTime());
        }
        return aggregate;
    }

}
