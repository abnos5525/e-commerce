import { ProtobufService } from './protobuf.service';
import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Partitioners, Producer } from 'kafkajs';
import {
  KAFKA_CONFIG,
  KAFKA_TOPIC_CONFIGS,
  KAFKA_TOPICS,
} from './kafka.config';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);
  private readonly kafka = new Kafka(KAFKA_CONFIG);
  private readonly producer: Producer = this.kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });

  constructor(private readonly protobufService: ProtobufService) {}

  async onModuleInit() {
    await this.ensureTopics();
    await this.producer.connect();
    await this.protobufService.load();
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }

  async sendOrderCreated(order: any) {
    await this.producer.send({
      topic: KAFKA_TOPICS.ORDERS,
      messages: [
        {
          key: String(order.id),
          value: this.protobufService.encode(order),
        },
      ],
    });
  }

  private async ensureTopics() {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const existing = new Set(await admin.listTopics());
      const missing = KAFKA_TOPIC_CONFIGS.filter((t) => !existing.has(t.topic));

      if (missing.length === 0) {
        this.logger.log('All Kafka topics already exist');
        return;
      }

      await admin.createTopics({ topics: missing, waitForLeaders: true });
      this.logger.log(
        `Created Kafka topics: ${missing.map((t) => t.topic).join(', ')}`,
      );
    } finally {
      await admin.disconnect();
    }
  }
}
