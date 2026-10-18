package com.example.analytics_service.query;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@Service
public class KafkaStoreQueryService {

    private final KafkaStreams kafkaStreams;

    public KafkaStoreQueryService(
            StreamsBuilderFactoryBean factoryBean
    ) {
        this.kafkaStreams =
                factoryBean.getKafkaStreams();
    }

    public Double getProductSales(
            String product
    ) {
        ReadOnlyKeyValueStore<String, Double> store =
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(
                                "product-sales-store",
                                org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore()
                        )
                );
        return store.get(product);
    }
}
