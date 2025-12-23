import { Controller, Post, Get, Body, UploadedFile, UseInterceptors, Query, HttpException, HttpStatus } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { UsersService } from './users.service';
import csvParser from 'csv-parser';
import { Readable } from 'stream';

@Controller('api')
export class UsersController {
  constructor(private readonly usersService: UsersService) {}

  @Post('upload-phonefile')
  @UseInterceptors(FileInterceptor('file'))
  async uploadPhoneFile(@UploadedFile() file: Express.Multer.File) {
    if (!file) throw new HttpException('File is required', HttpStatus.BAD_REQUEST);
    const text = file.buffer.toString('utf8').trim();
    let phones: string[] = [];

    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        if (parsed.length > 0 && typeof parsed[0] === 'string') {
          phones = parsed;
        } else {
          phones = parsed.map((p: any) => p.phoneNo || p.phone || p.Phone || p.mobile || '').filter(Boolean);
        }
      }
    } catch (e) {
      phones = [];
      const stream = Readable.from([text]);
      await new Promise<void>((resolve, reject) => {
        stream
          .pipe(csvParser())
          .on('data', (row) => {
            const val = row.phoneNo || row.phone || row.Phone || row.mobile || row.number || Object.values(row)[0];
            if (val) phones.push(val);
          })
          .on('end', () => resolve())
          .on('error', (err) => reject(err));
      });
    }

    phones = Array.from(new Set(phones.map(p => (p || '').replace(/[^\d+]/g, '').trim()).filter(Boolean)));
    if (!phones.length) return { matched: [] };

    const matchedUsers = await this.usersService.findUsersByPhones(phones);
    const businessIds = matchedUsers
      .map(u => (u.businessId ? u.businessId.toString() : null))
      .filter((id): id is string => !!id);
    const businesses = businessIds.length ? await this.usersService.findBusinessesByIds(businessIds) : [];
    const businessSet = new Set(businesses.map(b => b._id.toString()));

    const response = matchedUsers.map(u => ({
      userId: u._id.toString(),
      name: u.name || u.firstName || `${u.firstName || ''} ${u.lastName || ''}`.trim(),
      phoneNo: u.phoneNo,
      businessId: u.businessId ? u.businessId.toString() : null,
      businessExists: u.businessId ? businessSet.has(u.businessId.toString()) : false,
    }));

    return { matched: response };
  }

  @Post('delete-selected')
  async deleteSelected(@Body() body: { userIds: string[] }) {
    if (!body || !Array.isArray(body.userIds) || body.userIds.length === 0) {
      throw new HttpException('userIds required', HttpStatus.BAD_REQUEST);
    }
    const results = await this.usersService.transactionalDelete(body.userIds);
    return { results };
  }

  @Post('delete-by-phone')
  async deleteByPhone(@Body() body: { phoneNo: string }) {
    if (!body || !body.phoneNo) throw new HttpException('phoneNo required', HttpStatus.BAD_REQUEST);
    const matched = await this.usersService.findByPhone(body.phoneNo);
    if (!matched || matched.length === 0) {
      return { results: [], message: 'No users with this phone' };
    }
    const ids = matched.map((m: any) => m._id.toString());
    const results = await this.usersService.transactionalDelete(ids);
    return { results };
  }

  @Post('delete-by-email')
  async deleteByEmail(@Body() body: { email: string }) {
    if (!body || !body.email) throw new HttpException('email required', HttpStatus.BAD_REQUEST);
    const results = await this.usersService.deleteByEmailWithBackup(body.email);
    return { results };
  }

  @Post('delete-by-phone/sandbox')
  async deleteByPhoneSandbox(@Body() body: { phoneNo: string }) {
    if (!body || !body.phoneNo) throw new HttpException('phoneNo required', HttpStatus.BAD_REQUEST);
    const dbName = process.env.SANDBOX_DB || 'sandboxDb';
    const backupDbName = process.env.BACKUP_SANDBOX_DB || 'backUpSandboxDb';
    const result = await this.usersService.deleteByPhoneFromDb(body.phoneNo, dbName, backupDbName);
    return { result };
  }

  @Post('delete-by-phone/pepagora')
  async deleteByPhonePepagora(@Body() body: { phoneNo: string }) {
    if (!body || !body.phoneNo) throw new HttpException('phoneNo required', HttpStatus.BAD_REQUEST);
    const dbName = process.env.PEPAGORA_DB || 'pepagoraDb';
    const backupDbName = process.env.BACKUP_PEPAGORA_DB || 'backupPepagoraDb';
    const result = await this.usersService.deleteByPhoneFromDb(body.phoneNo, dbName, backupDbName);
    return { result };
  }

  @Post('delete-by-email/sandbox')
  async deleteByEmailSandbox(@Body() body: { email: string }) {
    if (!body || !body.email) throw new HttpException('email required', HttpStatus.BAD_REQUEST);
    const dbName = process.env.SANDBOX_DB || 'sandboxDb';
    const backupDbName = process.env.BACKUP_SANDBOX_DB || 'backUpSandboxDb';
    const result = await this.usersService.deleteByEmailFromDb(body.email, dbName, backupDbName);
    return { result };
  }

  @Post('delete-by-email/pepagora')
  async deleteByEmailPepagora(@Body() body: { email: string }) {
    if (!body || !body.email) throw new HttpException('email required', HttpStatus.BAD_REQUEST);
    const dbName = process.env.PEPAGORA_DB || 'pepagoraDb';
    const backupDbName = process.env.BACKUP_PEPAGORA_DB || 'backupPepagoraDb';
    const result = await this.usersService.deleteByEmailFromDb(body.email, dbName, backupDbName);
    return { result };
  }

  @Post('delete-user-related-data/sandbox')
  async deleteUserRelatedDataSandbox(@Body() body: { phoneNo?: string; email?: string }) {
    if (!body || (!body.phoneNo && !body.email)) {
      throw new HttpException('Either phoneNo or email is required', HttpStatus.BAD_REQUEST);
    }
    const dbName = process.env.SANDBOX_DB || 'sandboxDb';
    const backupDbName = process.env.BACKUP_SANDBOX_DB || 'backUpSandboxDb';
    const result = await this.usersService.deleteUserRelatedDataFromDb(
      dbName,
      backupDbName,
      body.phoneNo,
      body.email,
    );
    return { result };
  }

  @Post('delete-user-related-data/pepagora')
  async deleteUserRelatedDataPepagora(@Body() body: { phoneNo?: string; email?: string }) {
    if (!body || (!body.phoneNo && !body.email)) {
      throw new HttpException('Either phoneNo or email is required', HttpStatus.BAD_REQUEST);
    }
    const dbName = process.env.PEPAGORA_DB || 'pepagoraDb';
    const backupDbName = process.env.BACKUP_PEPAGORA_DB || 'backupPepagoraDb';
    const result = await this.usersService.deleteUserRelatedDataFromDb(
      dbName,
      backupDbName,
      body.phoneNo,
      body.email,
    );
    return { result };
  }

  @Post('delete-all-from-file')
  async deleteAllFromFile(@Body() body: { phoneNumbers: string[] }) {
    if (!body || !Array.isArray(body.phoneNumbers) || body.phoneNumbers.length === 0) {
      throw new HttpException('phoneNumbers array required', HttpStatus.BAD_REQUEST);
    }
    
    // Find all users matching the phone numbers from the file
    const matchedUsers = await this.usersService.findUsersByPhones(body.phoneNumbers);
    if (!matchedUsers || matchedUsers.length === 0) {
      return { results: [], message: 'No users found matching the provided phone numbers' };
    }
    
    const userIds = matchedUsers.map(user => user._id.toString());
    const results = await this.usersService.transactionalDelete(userIds);
    return { results, deletedCount: results.filter(r => r.userDeleted).length };
  }

  @Get('matches')
  async getMatches(@Query('phone') phone: string) {
    if (!phone) throw new HttpException('phone query required', HttpStatus.BAD_REQUEST);
    const matched = await this.usersService.findByPhone(phone);
    return {
      matched: matched.map(u => ({
        userId: u._id.toString(),
        name: u.name || u.firstName || '',
        phoneNo: u.phoneNo,
        businessId: u.businessId ? u.businessId.toString() : null,
      })),
    };
  }
}
