
package com.example.analytics_service.timestamp;

import com.example.analytics_service.proto.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderTimestampExtractorTest {

    private final OrderTimestampExtractor extractor =
            new OrderTimestampExtractor();

    @Test
    void shouldExtractCreatedAtFromEvent() {
        long expectedTimestamp = 1_700_000_000_000L;

        OrderCreatedEvent event = OrderCreatedEvent.newBuilder()
                .setCreatedAt(expectedTimestamp)
                .build();

        ConsumerRecord<Object, Object> record =
                new ConsumerRecord<>(
                        "orders",
                        0,
                        0L,
                        null,
                        event.toByteArray()
                );

        long actualTimestamp = extractor.extract(record, -1L);

        assertEquals(expectedTimestamp, actualTimestamp);
    }

    @Test
    void shouldRejectZeroTimestamp() {
        OrderCreatedEvent event = OrderCreatedEvent.newBuilder()
                .build();

        ConsumerRecord<Object, Object> record =
                new ConsumerRecord<>(
                        "orders",
                        0,
                        0L,
                        null,
                        event.toByteArray()
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> extractor.extract(record, -1L)
        );
    }
}
