package com.example.analytics_service.config;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {

        Map<String, Object> props = new HashMap<>();

        props.put(
            org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG,
            "analytics-service-group"
        );

        props.put(
            org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092"
        );

        props.put(
            org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        props.put(
            org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        return new KafkaStreamsConfiguration(props);
    }
}
