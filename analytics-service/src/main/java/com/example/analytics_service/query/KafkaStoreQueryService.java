package com.example.analytics_service.query;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class KafkaStoreQueryService {

    private final StreamsBuilderFactoryBean factoryBean;

    public KafkaStoreQueryService(
            StreamsBuilderFactoryBean factoryBean
    ) {
        this.factoryBean = factoryBean;
    }

    public Map<Long, Double> getProductSales(
            String product,
            long from,
            long to
    ) {
        if (from > to) {
            throw new IllegalArgumentException(
                    "'from' must be less than or equal to 'to'"
            );
        }

        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

        if (kafkaStreams == null) {
            throw new IllegalStateException(
                    "Kafka Streams has not been initialized"
            );
        }

        ReadOnlyWindowStore<String, ValueAndTimestamp<Double>> store =
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(
                                "product-sales-window-store",
                                QueryableStoreTypes.timestampedWindowStore()
                        )
                );

        Map<Long, Double> salesByWindowStart = new LinkedHashMap<>();

        try (
                WindowStoreIterator<ValueAndTimestamp<Double>> windows =
                        store.fetch(
                                product,
                                Instant.ofEpochMilli(from),
                                Instant.ofEpochMilli(to)
                        )
        ) {
            while (windows.hasNext()) {
                KeyValue<Long, ValueAndTimestamp<Double>> entry =
                        windows.next();

                salesByWindowStart.put(
                        entry.key,
                        entry.value.value()
                );
            }
        }

        return salesByWindowStart;
    }
}
