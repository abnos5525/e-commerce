import { ITopicConfig, KafkaConfig } from 'kafkajs';

export const KAFKA_CONFIG: KafkaConfig = {
  clientId: 'order-service',
  brokers: ['localhost:9092'],
};

export const KAFKA_TOPICS = {
  ORDERS: 'orders',
} as const;

export const KAFKA_TOPIC_CONFIGS: ITopicConfig[] = [
  { topic: KAFKA_TOPICS.ORDERS, numPartitions: 1, replicationFactor: 1 },
];
