package com.example.analytics_service.stream;

import com.example.analytics_service.model.OrderEvent;
import com.example.analytics_service.proto.OrderCreatedEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import java.time.Duration;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
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

    orders
        .mapValues(this::convertToOrder)
        .groupBy(
            (key, order) -> order.getProduct(),
            Grouped.with(
                Serdes.String(),
                Serdes.serdeFrom(
                    new org.springframework.kafka.support.serializer.JsonSerializer<>(),
                    new org.springframework.kafka.support.serializer.JsonDeserializer<>(OrderEvent.class))))
        .windowedBy(
            TimeWindows.ofSizeAndGrace(
                Duration.ofMinutes(5),
                Duration.ofMinutes(1)))
        .aggregate(
            () -> 0.0,
            (product, order, total) -> total + order.getPrice(),
            Materialized
                .<String, Double, WindowStore<Bytes, byte[]>>as("product-sales-window-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Double()))
        .toStream()
        .foreach(
            (windowedKey, total) -> {

              log.info(
                  "PRODUCT: {} WINDOW: {} - {} TOTAL: {}",
                  windowedKey.key(),
                  windowedKey.window().startTime(),
                  windowedKey.window().endTime(),
                  total);

            });

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

      throw new IllegalArgumentException(
          "Cannot decode protobuf",
          e);
    }
  }
}
