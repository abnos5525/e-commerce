import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class PaymentService {
  private readonly logger = new Logger(PaymentService.name);

  processPayment(order: any) {
    this.logger.log(`Processing payment for order: ${order.id}`);

    this.logger.log(`Payment completed: ${order.price}$`);
  }
}
