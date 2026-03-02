import { Controller, Post, Body, HttpException, HttpStatus } from '@nestjs/common';
import { UsersService } from './users.service';

@Controller('api')
export class UsersController {
  constructor(private readonly usersService: UsersService) {}


  @Post('delete-by-phone/sandbox')
  async deleteByPhoneSandbox(@Body() body: { phoneNo: string }) {
    if (!body || !body.phoneNo) throw new HttpException('phoneNo required', HttpStatus.BAD_REQUEST);
    const result = await this.usersService.deleteByPhone(body.phoneNo);
    return { result };
  }

  @Post('delete-by-email/sandbox')
  async deleteByEmailSandbox(@Body() body: { email: string }) {
    if (!body || !body.email) throw new HttpException('email required', HttpStatus.BAD_REQUEST);
    const result = await this.usersService.deleteByEmail(body.email);
    return { result };
  }

  @Post('delete-user-related-data/sandbox')
  async deleteUserRelatedDataSandbox(@Body() body: { phoneNo?: string; email?: string }) {
    if (!body || (!body.phoneNo && !body.email)) {
      throw new HttpException('Either phoneNo or email is required', HttpStatus.BAD_REQUEST);
    }
    const result = await this.usersService.deleteUserRelatedData(body.phoneNo, body.email);
    return { result };
  }

}
