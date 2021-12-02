package ru.on8off.kafka.streams.configuration.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class KafkaStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error(">>> Uncaught exception: {}",exception.getMessage(), exception);
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
