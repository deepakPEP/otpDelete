import { Injectable } from '@nestjs/common';
import { InjectModel, InjectConnection } from '@nestjs/mongoose';
import { Model, Connection } from 'mongoose';
import { Otp, OtpSchema } from './otp.schema';

@Injectable()
export class OtpsService {
  constructor(
    @InjectModel(Otp.name) private otpModel: Model<Otp>,
    @InjectConnection() private connection: Connection,
  ) {}

  async findLatestOtpByPhone(phoneNo: string): Promise<string | null> {
    const latestOtpDoc = await this.otpModel
      .findOne({ phoneNo })
      .sort({ createdAt: -1 })
      .exec();

    return latestOtpDoc ? latestOtpDoc.otp : null;
  }

  async findLatestOtpByPhoneFromDb(phoneNo: string, dbName: string): Promise<string | null> {
    // Switch to the specified database
    const dbConnection = this.connection.useDb(dbName, { useCache: true });
    
    // Create OTP model on this database connection
    const otpModel = dbConnection.model(Otp.name, OtpSchema);
    
    // Query with same logic: find by phoneNo, sort by createdAt descending
    const latestOtpDoc = await otpModel
      .findOne({ phoneNo })
      .sort({ createdAt: -1 })
      .exec();

    return latestOtpDoc ? latestOtpDoc.otp : null;
  }
}