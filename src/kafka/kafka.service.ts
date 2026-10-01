import { Injectable, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaService implements OnModuleInit {
  private producer!: Producer;

  async onModuleInit() {
    const kafka = new Kafka({
      clientId: 'order-service',
      brokers: ['localhost:9092'],
    });

    this.producer = kafka.producer();
    await this.producer.connect();
  }

  async sendOrderCreated(order: any) {
    await this.producer.send({
      topic: 'orders',
      messages: [
        {
          key: String(order.id),
          value: JSON.stringify(order),
        },
      ],
    });
  }
}
