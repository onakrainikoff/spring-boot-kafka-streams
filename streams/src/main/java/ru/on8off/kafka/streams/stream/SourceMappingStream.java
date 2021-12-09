package ru.on8off.kafka.streams.stream;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;
import ru.on8off.kafka.streams.stream.mapper.PaymentMapper;

@Service
@Slf4j
public class SourceMappingStream {
    @Value("${topics.source}")
    private String sourceTopic;
    @Value("${topics.payments}")
    private String paymentsTopic;
    @Autowired
    private SpecificAvroSerde<PaymentEventAvro> paymentEventAvroSerde;
    @Autowired
    private Serde<Payment> paymentSerde;


    public void stream(StreamsBuilder builder) {
        TimestampExtractor timestampExtractor = (record, partitionTime) -> ((PaymentEventAvro) record.value()).getTimestamp();
        var sourceStream = builder.stream(
                sourceTopic,
                Consumed.with(Serdes.Long(), paymentEventAvroSerde)
                .withTimestampExtractor(timestampExtractor)
            ).peek((key, value) -> log.info(">>> sourceMappingAndSplitStream consuming: key={}, value={}", key, value));
        sourceStream.mapValues(new PaymentMapper())
                .peek((key, value) -> log.info(">>> sourceMappingAndSplitStream produce to paymentsTopic: key={}, value={}", key, value))
                .to(paymentsTopic, Produced.with(Serdes.Long(), paymentSerde));
    }
}
