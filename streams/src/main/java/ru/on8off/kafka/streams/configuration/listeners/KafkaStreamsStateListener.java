package ru.on8off.kafka.streams.configuration.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
public class KafkaStreamsStateListener implements KafkaStreams.StateListener{
    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        log.info(">>> onChange state newState={} oldState={}", newState, oldState);
    }
}
