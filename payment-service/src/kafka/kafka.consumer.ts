import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Consumer } from 'kafkajs';
import { KAFKA_CONFIG, KAFKA_TOPICS } from './kafka.config';
import { PaymentService } from '../payment/payment.service';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumer.name);
  private readonly kafka = new Kafka(KAFKA_CONFIG);
  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: 'payment-group',
  });

  constructor(private readonly paymentService: PaymentService) {}

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: KAFKA_TOPICS.ORDERS,
      fromBeginning: true,
    });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const order = JSON.parse(message.value!.toString());

        this.logger.log(
          `
          Topic: ${topic}
          Partition: ${partition}
          Offset: ${message.offset}
          Order: ${JSON.stringify(order)}
          `,
        );

        this.paymentService.processPayment(order);
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }
}
