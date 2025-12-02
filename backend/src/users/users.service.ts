import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types, ClientSession } from 'mongoose';
import { User } from './schemas/user.schema';
import { Business } from './schemas/business.schema';

@Injectable()
export class UsersService {
  private readonly logger = new Logger(UsersService.name);

  constructor(
    @InjectModel(User.name) private userModel: Model<User>,
    @InjectModel(Business.name) private businessModel: Model<Business>,
  ) {}

  normalizePhone(phone?: string) {
    if (!phone) return phone;
    return phone.replace(/[^\d+]/g, '');
  }

  async findUsersByPhones(phoneList: string[]) {
    const normalized = phoneList.map(p => this.normalizePhone(p)).filter(Boolean);
    return this.userModel.find({ phoneNo: { $in: normalized } }).lean();
  }

  async findBusinessesByIds(ids: (string | Types.ObjectId)[]) {
    const objectIds = ids.map(id => new Types.ObjectId(id));
    return this.businessModel.find({ _id: { $in: objectIds } }).lean();
  }

  async findByPhone(phone: string) {
    const norm = this.normalizePhone(phone);
    return this.userModel.find({ phoneNo: norm }).lean();
  }

  async deleteUsersAndBusinesses(
    userIds: string[],
    session?: ClientSession
  ): Promise<
    Array<
      | { userId: string; userDeleted: boolean; businessDeleted: boolean; error: string }
      | { userId: string; businessId: string | null; userDeleted: boolean; businessDeleted: boolean }
    >
  > {
    const results: Array<
      | { userId: string; userDeleted: boolean; businessDeleted: boolean; error: string }
      | { userId: string; businessId: string | null; userDeleted: boolean; businessDeleted: boolean }
    > = [];
    
    for (const uid of userIds) {
      try {
        const user = await this.userModel.findById(uid).session(session ?? null);
        if (!user) {
          results.push({ userId: uid, userDeleted: false, businessDeleted: false, error: 'User not found' });
          continue;
        }
        
        const businessId = user.businessId ? user.businessId.toString() : null;

        // Delete user first
        const userDeleteResult = await this.userModel.deleteOne({ _id: user._id }).session(session ?? null);

        let businessDeleted = false;
        if (businessId) {
          // Check if any other users still reference this business
          const otherUsersCount = await this.userModel.countDocuments({ 
            businessId: user.businessId, 
            _id: { $ne: user._id } 
          }).session(session ?? null);
          
          // If no other users reference this business, delete it
          if (otherUsersCount === 0) {
            const businessDeleteResult = await this.businessModel.deleteOne({ 
              _id: user.businessId 
            }).session(session ?? null);
            businessDeleted = businessDeleteResult.deletedCount === 1;
            this.logger.log(`Business ${businessId} deleted: ${businessDeleted}`);
          } else {
            this.logger.log(`Business ${businessId} not deleted - still referenced by ${otherUsersCount} other user(s)`);
          }
        }

        results.push({
          userId: uid,
          businessId,
          userDeleted: userDeleteResult.deletedCount === 1,
          businessDeleted,
        });
        
        this.logger.log(`User ${uid} deleted: ${userDeleteResult.deletedCount === 1}, Business ${businessId} deleted: ${businessDeleted}`);
        
      } catch (err) {
        this.logger.error(`Error deleting user ${uid}: ${err.message}`);
        results.push({ userId: uid, userDeleted: false, businessDeleted: false, error: err.message });
      }
    }
    return results;
  }

  async transactionalDelete(userIds: string[]) {
    const conn = this.userModel.db;
    const session = await conn.startSession();
    const results: Array<
      | { userId: string; userDeleted: boolean; businessDeleted: boolean; error: string }
      | { userId: string; businessId: string | null; userDeleted: boolean; businessDeleted: boolean }
    > = [];
    try {
      await session.withTransaction(async () => {
        const res = await this.deleteUsersAndBusinesses(userIds, session);
        results.push(...res);
      });
    } catch (err) {
      this.logger.error('Transaction error: ' + err.message);
      const fallback = await this.deleteUsersAndBusinesses(userIds);
      return fallback;
    } finally {
      session.endSession();
    }
    return results;
  }
}
