import { ProtobufService } from './../../../src/kafka/protobuf.service';
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

  constructor(
    private readonly paymentService: PaymentService,
    private readonly protobufService: ProtobufService,
  ) {}

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: KAFKA_TOPICS.ORDERS,
      fromBeginning: true,
    });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        const order = this.protobufService.decode(message.value!);

        this.logger.log(
          `
          Topic: ${topic}
          Partition: ${partition}
          Offset: ${message.offset}
          Order: ${JSON.stringify(order)}
          `,
        );

        this.paymentService.processPayment(order);

        await this.consumer.commitOffsets([
          {
            topic,
            partition,
            offset: String(Number(message.offset) + 1),
          },
        ]);

        await heartbeat();
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }
}
