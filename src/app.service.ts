import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { KafkaService } from './kafka/kafka.service';

@Injectable()
export class AppService {
  constructor(private readonly kafkaService: KafkaService) {}

  async sendOrderCreated() {
    await this.kafkaService.sendOrderCreated({
      id: randomUUID(),
      product: 'Laptop',
      price: 2000,
    });
  }
}
