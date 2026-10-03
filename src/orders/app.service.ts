import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { CreateOrderDto } from '../dto/create-order.dto';
import { KafkaService } from '../kafka/kafka.service';

@Injectable()
export class AppService {
  constructor(private readonly kafkaService: KafkaService) {}

  async sendOrderCreated(dto: CreateOrderDto) {
    const order = {
      id: randomUUID(),
      paymentId: randomUUID(),
      ...dto,
    };

    await this.kafkaService.sendOrderCreated(order);
    return order;
  }
}
