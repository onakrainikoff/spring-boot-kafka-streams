package ru.on8off.kafka.streams.configuration;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.on8off.kafka.streams.model.Payment;
import ru.on8off.kafka.streams.model.avro.PaymentEventAvro;

import java.util.Map;

@Configuration
public class SerdesConfiguration {
    @Value("${schema-registry}")
    private String schemaRegistry;

    @Bean
    public SpecificAvroSerde<PaymentEventAvro> paymentEventAvroSerde() {
        var recordSerde = new SpecificAvroSerde<PaymentEventAvro>();
        recordSerde.configure(Map.of(
                "schema.registry.url", schemaRegistry,
                "auto.register.schemas", "true"
        ), false);
        return recordSerde;
    }

    @Bean
    public Serde<Payment> paymentSerde(){
        var deserializer = new JsonDeserializer<>(Payment.class);
        deserializer.addTrustedPackages("*");
        var serializer = new JsonSerializer<Payment>();
        return new JsonSerde<>(serializer, deserializer);
    }

}
