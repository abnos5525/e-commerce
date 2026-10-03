package com.example.analytics_service.timestamp;

import com.example.analytics_service.proto.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class OrderTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(
            ConsumerRecord<Object, Object> record,
            long partitionTime
    ) {
        try {
            byte[] payload = (byte[]) record.value();

            OrderCreatedEvent event =
                    OrderCreatedEvent.parseFrom(payload);

            return event.getCreatedAt();
        } catch (Exception e) {
            return partitionTime;
        }
    }
}
