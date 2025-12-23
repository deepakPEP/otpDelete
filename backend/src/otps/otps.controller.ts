import { Controller, NotFoundException, Body, Post } from '@nestjs/common';
import { OtpsService } from './otps.service';

@Controller('findOtp')
export class OtpsController {
  constructor(private readonly otpsService: OtpsService) {}

  @Post('sandbox')
  async getOtp(@Body('phoneNo') phoneNo: string) {
    if (!phoneNo) {
      throw new NotFoundException('phoneNo is required in the request body');
    }
    const otp = await this.otpsService.findLatestOtpByPhone(phoneNo);
    if (!otp) {
      throw new NotFoundException(`No OTP found for ${phoneNo}`);
    }
    return { phoneNo, otp, database: 'sandbox' };
  }

  @Post('pepagora')
  async getOtpFromPepagora(@Body('phoneNo') phoneNo: string) {
    if (!phoneNo) {
      throw new NotFoundException('phoneNo is required in the request body');
    }
    const dbName = process.env.PEPAGORA_DB || 'pepagoraDb';
    const otp = await this.otpsService.findLatestOtpByPhoneFromDb(phoneNo, dbName);
    if (!otp) {
      throw new NotFoundException(`No OTP found for ${phoneNo} in ${dbName}`);
    }
    return { phoneNo, otp, database: dbName };
  }
}