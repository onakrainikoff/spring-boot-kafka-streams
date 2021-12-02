package ru.on8off.kafka.streams.strem;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.on8off.kafka.streams.model.Payment;

@Service
@Slf4j
public class PaymentSucceedStream {
    @Value("${topics.paymentsSuccess}")
    private String paymentsSuccessTopic;

    @Autowired
    private Serde<Payment> paymentSerde;

    public void stream(StreamsBuilder builder) {
        var paymentsStream = builder.stream(paymentsSuccessTopic, Consumed.with(Serdes.Long(), paymentSerde))
                .peek((aLong, siteVisit) -> log.info(">>> PaymentSucceedStream consuming: key={}, value={}", aLong, siteVisit));
    }

}
