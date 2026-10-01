import { KafkaConfig } from 'kafkajs';

export const KAFKA_CONFIG: KafkaConfig = {
  clientId: 'payment-service',

  brokers: ['localhost:9092'],
};

export const KAFKA_TOPICS = {
  ORDERS: 'orders',
} as const;
