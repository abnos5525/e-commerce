package com.example.analytics_service.stream;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class OrderStreamProcessor {

    @Bean
    public KStream<String, String> processOrders(
            StreamsBuilder builder
    ) {
        KStream<String, String> orders =
                builder.stream("orders");

        orders.foreach(
                (key, value) -> {
                    System.out.println(
                            "NEW ORDER EVENT: "
                                    + value
                    );
                }
        );
        return orders;
    }
}
