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
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderStreamProcessor {

  @Bean
  public KStream<String, byte[]> processOrders(StreamsBuilder builder) {
    KStream<String, byte[]> orders =
        builder.stream("orders", Consumed.with(Serdes.String(), Serdes.ByteArray()));

    orders
        .mapValues(payload -> convertToOrder(payload).getPrice())
        .groupBy((key, price) -> "TOTAL", Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(
            (total, price) -> total + price,
            Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("sales-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Double()))
        .toStream()
        .foreach((key, total) -> log.info("TOTAL SALES: {}", total));

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
      throw new IllegalArgumentException("Cannot decode protobuf", e);
    }
  }
}
