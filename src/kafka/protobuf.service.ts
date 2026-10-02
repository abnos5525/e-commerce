import { Injectable } from '@nestjs/common';
import { join } from 'node:path';
import protobuf from 'protobufjs';

@Injectable()
export class ProtobufService {
  private messageType: any;

  async load() {
    const protoPath = join(
      process.cwd(),
      'event-contracts',
      'order-created.proto',
    );
    const root = await protobuf.load(protoPath);

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
