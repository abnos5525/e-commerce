import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class PaymentService {
  private readonly logger = new Logger(PaymentService.name);

  private processedPayments = new Set<string>();

  processPayment(order: any) {
    const paymentId = order.paymentId as string;

    if (this.processedPayments.has(paymentId)) {
      this.logger.warn(`Payment already processed: ${paymentId}`);
      return;
    }

    this.logger.log(`Processing payment for order: ${order.id}`);

    this.processedPayments.add(paymentId);

    this.logger.log(`Payment completed: ${order.price}$`);
  }
}
