import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { UsersModule } from './users/users.module';
import { OtpsModule } from './otps/otps.module';
import * as dotenv from 'dotenv';
dotenv.config();

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI ?? (() => { throw new Error('MONGO_URI is not defined'); })(), {
      dbName: process.env.MONGO_DB || 'sandboxDb',
      autoCreate: true,
    }),
    UsersModule,
    OtpsModule,
  ],
})
export class AppModule {}
