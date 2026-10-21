package com.example.analytics_service.timestamp;

import com.example.analytics_service.proto.OrderCreatedEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class OrderTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(
      ConsumerRecord<Object, Object> record,
      long partitionTime) {
    if (!(record.value() instanceof byte[] payload)) {
      throw new IllegalArgumentException(
          "Expected byte[] payload for OrderCreatedEvent");
    }

    final OrderCreatedEvent event;
    try {
      event = OrderCreatedEvent.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Cannot decode OrderCreatedEvent",
          e);
    }

    long createdAt = event.getCreatedAt();

    if (createdAt <= 0) {
      throw new IllegalArgumentException(
          "Invalid createdAt: " + createdAt);
    }

    return createdAt;
  }
}
