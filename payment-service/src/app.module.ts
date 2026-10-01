import { Module } from '@nestjs/common';
import { KafkaConsumer } from './kafka/kafka.consumer';
import { PaymentService } from './payment/payment.service';

@Module({
  imports: [],
  controllers: [],
  providers: [KafkaConsumer, PaymentService],
})
export class AppModule {}
