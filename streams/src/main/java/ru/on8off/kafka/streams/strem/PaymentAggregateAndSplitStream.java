package ru.on8off.kafka.streams.strem;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.strem.transformer.PaymentAggregationTransformer;

@Service
@Slf4j
public class PaymentAggregateAndSplitStream {
    @Value("${topics.payments}")
    private String paymentsTopic;
    @Value("${topics.paymentsSuccess}")
    private String paymentsSuccessTopic;
    @Value("${topics.paymentsFiled}")
    private String paymentsFiledTopic;
    @Value("${topics.paymentsAbandoned}")
    private String paymentsAbandonedTopic;
    private String PAYMENT_STORE = "payment-aggregation-store";
    private Long AGGREGATION_WINDOW = 1000L;

    @Autowired
    private Serde<Payment> paymentSerde;

    public void stream(StreamsBuilder builder) {
        var paymentsStream = builder.stream(paymentsTopic, Consumed.with(Serdes.Long(), paymentSerde))
                .peek((aLong, siteVisit) -> log.info(">>> paymentAggregateAndSplitStream consuming: key={}, value={}", aLong, siteVisit));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(PAYMENT_STORE),
                Serdes.Long(),
                paymentSerde
        ));

        var paymentsStreamTransformed = paymentsStream.transform(
                ()->  new PaymentAggregationTransformer(PAYMENT_STORE, AGGREGATION_WINDOW),
                PAYMENT_STORE
        );

        subStreamForSucceed(paymentsStreamTransformed);
        subStreamForFailed(paymentsStreamTransformed);
        subStreamForAbandoned(paymentsStreamTransformed);
    }

    private void subStreamForSucceed(KStream<Long, Payment> paymentsStream) {
        paymentsStream
                .filter((key, value) -> value.getPaymentSucceedTime() != null)
                .peek((key, value) -> log.info(">>> paymentAggregateAndSplitStream produce to paymentsSuccessTopic: key={}, value={}", key, value))
                .to(paymentsSuccessTopic, Produced.with(Serdes.Long(), paymentSerde));
    }

    private void subStreamForFailed(KStream<Long, Payment> paymentsStream) {
        paymentsStream
                .filter((key, value) -> value.getPaymentFailedTime() != null)
                .peek((key, value) -> log.info(">>> paymentAggregateAndSplitStream produce to paymentsFiledTopic: key={}, value={}", key, value))
                .to(paymentsFiledTopic, Produced.with(Serdes.Long(), paymentSerde));
    }

    private void subStreamForAbandoned(KStream<Long, Payment> paymentsStream) {
        paymentsStream
                .filter((key, value) -> value.getPaymentSucceedTime() == null && value.getPaymentFailedTime() == null)
                .peek((key, value) -> log.info(">>> paymentAggregateAndSplitStream produce to paymentsAbandonedTopic: key={}, value={}", key, value))
                .to(paymentsAbandonedTopic, Produced.with(Serdes.Long(), paymentSerde));
    }
}
