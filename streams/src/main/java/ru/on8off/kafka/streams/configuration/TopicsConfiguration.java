package ru.on8off.kafka.streams.configuration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TopicsConfiguration {
    @Value("${bootstrap-servers}")
    private String bootstrapServers;
    @Value("${partitions}")
    private Integer partitions;
    @Value("${replications}")
    private Short replications;
    @Value("${topics.payments}")
    private String paymentsTopic;
    @Value("${topics.paymentsSuccess}")
    private String paymentsSuccessTopic;
    @Value("${topics.paymentsFiled}")
    private String paymentsFiledTopic;
    @Value("${topics.paymentsAbandoned}")
    private String paymentsAbandonedTopic;

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic paymentsTopic() {
        return new NewTopic(paymentsTopic, partitions, replications);
    }


    @Bean
    public NewTopic paymentsSuccessTopic() {
        return new NewTopic(paymentsSuccessTopic, partitions, replications);
    }

    @Bean
    public NewTopic paymentsFiledTopic() {
        return new NewTopic(paymentsFiledTopic, partitions, replications);
    }

    @Bean
    public NewTopic paymentsAbandonedTopic() {
        return new NewTopic(paymentsAbandonedTopic, partitions, replications);
    }
}
