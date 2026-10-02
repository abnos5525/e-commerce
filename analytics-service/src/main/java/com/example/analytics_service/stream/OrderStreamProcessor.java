package com.example.analytics_service.stream;

import com.example.analytics_service.model.OrderEvent;
import com.example.analytics_service.proto.OrderCreatedEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.serializer.JsonSerde;

@Component
public class OrderStreamProcessor {

  @Bean
  public KStream<String, byte[]> processOrders(
      StreamsBuilder builder) {

    KStream<String, byte[]> orders = builder.stream(
        "orders",
        Consumed.with(
            Serdes.String(),
            Serdes.ByteArray()));

    KTable<String, Double> totalSales = orders
        .mapValues(this::convertToOrder)
        .groupBy(
            (key, order) -> "TOTAL",
            Grouped.with(
                Serdes.String(),
                new JsonSerde<>(OrderEvent.class)))
        .aggregate(
            () -> 0.0,
            (key, order, total) -> total + order.getPrice(),
            Materialized
                .<String, Double, KeyValueStore<Bytes, byte[]>>as("sales-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Double()));

    totalSales
        .toStream()
        .foreach(
            (key, total) -> System.out.println(
                "TOTAL SALES: "
                    + total));

    return orders;
  }

  private OrderEvent convertToOrder(byte[] payload) {

    try {

      OrderCreatedEvent proto = OrderCreatedEvent.parseFrom(payload);

      OrderEvent order = new OrderEvent();

      order.setId(proto.getId());
      order.setProduct(proto.getProduct());
      order.setPrice(proto.getPrice());

      return order;

    } catch (InvalidProtocolBufferException e) {

      throw new RuntimeException(
          "Cannot decode protobuf",
          e);

    }
  }
}
