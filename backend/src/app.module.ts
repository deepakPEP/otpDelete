import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { UsersModule } from './users/users.module';
import { OtpsModule } from './otps/otps.module';
import * as dotenv from 'dotenv';
dotenv.config();
// msanthoshhh

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI ?? (() => { throw new Error('MONGO_URI is not defined'); })(), {
      dbName: process.env.MONGO_DB || 'sandboxDb',
      autoCreate: true,
      serverSelectionTimeoutMS: 30000, // 30 seconds to select a server
      connectTimeoutMS: 30000, // 30 seconds to establish connection
      socketTimeoutMS: 45000, // 45 seconds for socket operations
      maxPoolSize: 10, // Maximum number of connections in the pool
      minPoolSize: 2, // Minimum number of connections in the pool
      retryWrites: true, // Retry write operations on network errors
      maxIdleTimeMS: 60000, // Close connections after 60 seconds of inactivity (increased from 30s)
      heartbeatFrequencyMS: 10000, // Send heartbeat every 10 seconds to keep connections alive
      serverMonitoringMode: 'auto', // Automatically monitor server status
    }),
    UsersModule,
    OtpsModule,
  ],
})
export class AppModule {}