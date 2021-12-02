package ru.on8off.kafka.streams.configuration.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

@Slf4j
public class KafkaStreamsStateRestoreListener implements StateRestoreListener {
    @Override
    public void onRestoreStart(TopicPartition topicPartition, String store, long start, long end) {
        log.info(">>> onRestoreStart: store={},  topicPartition={},  start={}, end={}", store, topicPartition, start, end);
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String store, long start, long batchCompleted) {
        log.info(">>> onBatchRestored: store={},  topicPartition={},  start={}, batchCompleted={}", store, topicPartition, start, batchCompleted);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String store, long totalRestored) {
        log.info(">>> onRestoreEnd: store={},  topicPartition={},  totalRestored={}", store, topicPartition, totalRestored);
    }
}
