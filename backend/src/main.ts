import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as dotenv from 'dotenv';
dotenv.config();

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: true, credentials: true });
  const port = process.env.PORT || 4000;
  
  // Log MongoDB connection info (masked for security)
  const mongoUri = process.env.MONGO_URI || 'not set';
  const maskedUri = mongoUri.includes('@') 
    ? mongoUri.split('@')[0].split(':').slice(0, 2).join(':') + ':***@' + mongoUri.split('@')[1]
    : mongoUri;
  console.log(`MongoDB URI: ${maskedUri}`);
  console.log(`MongoDB Database: ${process.env.MONGO_DB || 'sandboxDb'}`);
  
  await app.listen(port);
  console.log(`Server running on http://localhost:${port}`);
}
bootstrap();
