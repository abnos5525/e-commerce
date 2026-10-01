import { Body, Controller, Post } from '@nestjs/common';
import { ApiCreatedResponse, ApiTags } from '@nestjs/swagger';
import { AppService } from './app.service';
import { CreateOrderDto } from './dto/create-order.dto';

@ApiTags('orders')
@Controller('orders')
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Post()
  @ApiCreatedResponse({ description: 'Order created and sent to Kafka' })
  sendOrder(@Body() dto: CreateOrderDto) {
    return this.appService.sendOrderCreated(dto);
  }
}
