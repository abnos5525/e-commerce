import { Injectable } from '@nestjs/common';
import protobuf from 'protobufjs';

@Injectable()
export class ProtobufService {
  private messageType: any;

  async load() {
    const root = await protobuf.load(
      '../../event-contracts/order-created.proto',
    );

    this.messageType = root.lookupType('ecommerce.OrderCreatedEvent');
  }

  encode(data: any) {
    const message = this.messageType.create(data);

    return this.messageType.encode(message).finish();
  }

  decode(buffer: Buffer) {
    return this.messageType.decode(buffer);
  }
}
