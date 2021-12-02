package ru.on8off.kafka.streams.configuration;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.on8off.kafka.streams.strem.PaymentAbandonedStream;
import ru.on8off.kafka.streams.strem.PaymentAggregateAndSplitStream;
import ru.on8off.kafka.streams.strem.PaymentFailedStream;
import ru.on8off.kafka.streams.strem.PaymentSucceedStream;
import ru.on8off.kafka.streams.strem.SourceMappingStream;

@Configuration
public class TopologyConfiguration {
    @Autowired
    private SourceMappingStream sourceMappingStream;
    @Autowired
    private PaymentAggregateAndSplitStream paymentAggregateAndSplitStream;
    @Autowired
    private PaymentSucceedStream paymentSucceedStream;
    @Autowired
    private PaymentFailedStream paymentFailedStream;
    @Autowired
    private PaymentAbandonedStream paymentAbandonedStream;


    @Bean
    public Topology topology(StreamsBuilder streamsBuilder) {
        sourceMappingStream.stream(streamsBuilder);
        paymentAggregateAndSplitStream.stream(streamsBuilder);
        paymentSucceedStream.stream(streamsBuilder);
        paymentFailedStream.stream(streamsBuilder);
        paymentAbandonedStream.stream(streamsBuilder);
        return streamsBuilder.build();
    }
}
