import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Otp } from './otp.schema';

@Injectable()
export class OtpsService {
  constructor(@InjectModel(Otp.name) private otpModel: Model<Otp>) {}

  async findLatestOtpByPhone(phoneNo: string): Promise<string | null> {
    const latestOtpDoc = await this.otpModel
      .findOne({ phoneNo })
      .sort({ createdAt: -1 })
      .exec();

    return latestOtpDoc ? latestOtpDoc.otp : null;
  }
}