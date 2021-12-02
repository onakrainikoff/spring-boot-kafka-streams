package ru.on8off.kafka.streams.integration.configuration;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestConfiguration
public class TestKafkaConfiguration {
    @Value("${bootstrap-servers}")
    private String bootstrapServers;
    @Value("${schema-registry}")
    private String schemaRegistry;
    @Value("${topics.paymentsSuccess}")
    private String paymentsSuccessTopic;
    @Value("${topics.paymentsFiled}")
    private String paymentsFailedTopic;
    @Value("${topics.paymentsAbandoned}")
    private String paymentsAbandonedTopic;


    @Bean
    public KafkaTemplate<Long, PaymentEventAvro> avroPaymentEventKafkaTemplate(){
        var properties = new HashMap<String, Object>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 10000);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        properties.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(properties));
    }

    @Bean
    public KafkaConsumer<Long, Payment> paymentsSuccessKafkaConsumer() {
        var properties = getConsumerConfig("group-paymentsSuccessKafkaConsumer", Payment.class);
        var customer =  new KafkaConsumer<Long, Payment>(properties);
        customer.subscribe(List.of(paymentsSuccessTopic));
        return customer;
    }

    @Bean
    public KafkaConsumer<Long, Payment> paymentsAbandonedKafkaConsumer() {
        var properties = getConsumerConfig("group-paymentsAbandonedKafkaConsumer", Payment.class);
        var customer =  new KafkaConsumer<Long, Payment>(properties);
        customer.subscribe(List.of(paymentsAbandonedTopic));
        return customer;
    }

    @Bean
    public KafkaConsumer<Long, Payment> paymentsFailedKafkaConsumer() {
        var properties = getConsumerConfig("group-paymentsFailedKafkaConsumer", Payment.class);
        var customer =  new KafkaConsumer<Long, Payment>(properties);
        customer.subscribe(List.of(paymentsFailedTopic));
        return customer;
    }


    private Map<String, Object> getConsumerConfig(String group, Class clazz){
        var properties = new HashMap<String, Object>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        properties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, clazz);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return properties;
    }

}
