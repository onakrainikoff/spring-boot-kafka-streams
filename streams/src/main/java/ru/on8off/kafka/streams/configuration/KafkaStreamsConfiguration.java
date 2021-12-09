package ru.on8off.kafka.streams.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import ru.on8off.kafka.streams.configuration.listeners.KafkaStreamsStateListener;
import ru.on8off.kafka.streams.configuration.listeners.KafkaStreamsStateRestoreListener;
import ru.on8off.kafka.streams.configuration.listeners.KafkaStreamsUncaughtExceptionHandler;

import java.util.HashMap;


@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfiguration {
    @Value("${application.id}")
    private String applicationId;
    @Value("${bootstrap-servers}")
    private String bootstrapServers;
    @Value("${partitions}")
    private Integer partitions;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public org.springframework.kafka.config.KafkaStreamsConfiguration getStreamsConfig() {
        var properties = new HashMap<String, Object>();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, partitions);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new org.springframework.kafka.config.KafkaStreamsConfiguration(properties);
    }

    @Bean
    public StreamsBuilderFactoryBeanCustomizer streamsBuilderFactoryBeanCustomizer() {
        return factoryBean -> {
            factoryBean.setKafkaStreamsCustomizer( kafkaStreams -> {
                kafkaStreams.setGlobalStateRestoreListener(new KafkaStreamsStateRestoreListener());
                kafkaStreams.setUncaughtExceptionHandler(new KafkaStreamsUncaughtExceptionHandler());
                kafkaStreams.setStateListener(new KafkaStreamsStateListener());
            });
        };
    }
}
