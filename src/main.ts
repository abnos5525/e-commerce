import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const port = process.env.PORT ?? 3000;
  await app.listen(port, '127.0.0.1');
  console.log(`Application is running on: http://localhost:${port}`);
}
void bootstrap();
