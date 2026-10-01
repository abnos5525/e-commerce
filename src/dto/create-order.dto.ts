import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsNumber, IsPositive, IsString } from 'class-validator';

export class CreateOrderDto {
  @ApiProperty({ example: 'Laptop' })
  @IsString()
  @IsNotEmpty()
  product!: string;

  @ApiProperty({ example: 2000 })
  @IsNumber()
  @IsPositive()
  price!: number;
}
