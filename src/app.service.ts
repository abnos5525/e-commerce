import { Injectable } from '@nestjs/common';
import { KafkaService } from './kafka/kafka.service';

@Injectable()
export class AppService {
  constructor(private readonly kafkaService: KafkaService) {}

  async getHello() {
    await this.kafkaService.sendOrderCreated({
      id: 1,
      product: 'Laptop',
      price: 2000,
    });
  }
}
