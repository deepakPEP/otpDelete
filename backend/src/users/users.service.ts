import { Injectable, Logger } from '@nestjs/common';
import { InjectModel, InjectConnection } from '@nestjs/mongoose';
import { Model, Types, Connection } from 'mongoose';
import { User, UserSchema } from './schemas/user.schema';
import { Business, BusinessSchema } from './schemas/business.schema';

@Injectable()
export class UsersService {
  private readonly logger = new Logger(UsersService.name);

  constructor(
    @InjectModel(User.name) private userModel: Model<User>,
    @InjectModel(Business.name) private businessModel: Model<Business>,
    @InjectConnection() private connection: Connection,
  ) {}

  normalizePhone(phone?: string) {
    if (!phone) return phone;
    return phone.replace(/[^\d+]/g, '');
  }

  // -------------------------------
  // DELETE BY PHONE
  // -------------------------------
  async deleteByPhone(phoneNo: string): Promise<any> {
    return await this.deleteUserRelatedData(phoneNo, undefined);
  }

  // -------------------------------
  // DELETE BY EMAIL
  // -------------------------------
  async deleteByEmail(email: string): Promise<any> {
    return await this.deleteUserRelatedData(undefined, email);
  }

  // -------------------------------
  // MAIN DELETE FUNCTION (OPTIMIZED)
  // -------------------------------
  async deleteUserRelatedData(phoneNo?: string, email?: string): Promise<any> {
    const dbName = process.env.SANDBOX_DB || 'sandboxDb';
    const backupDbName = process.env.BACKUP_SANDBOX_DB || 'backUpSandboxDb';

    const result: any = {
      database: dbName,
      usersFound: 0,
      userIds: [],
      usersBackedUp: 0,
      usersDeleted: 0,
      businessesBackedUp: 0,
      businessesDeleted: 0,
      otpsBackedUp: 0,
      errors: [],
    };

    try {
      const sourceConnection = this.connection.useDb(dbName, { useCache: true });
      const backupConnection = this.connection.useDb(backupDbName, { useCache: true });

      const sourceUserModel = sourceConnection.model(User.name, UserSchema);
      const sourceBusinessModel = sourceConnection.model(Business.name, BusinessSchema);

      // -------------------------------
      // FIND USERS
      // -------------------------------
      let matchedUsers: any[] = [];

      if (phoneNo) {
        const norm = this.normalizePhone(phoneNo);
        matchedUsers = await sourceUserModel.find({ phoneNo: norm }).lean();
      } else if (email) {
        const normalizedEmail = email.trim().toLowerCase();
        matchedUsers = await sourceUserModel.find({
          $or: [
            { email: normalizedEmail },
            { workEmail: normalizedEmail },
          ],
        }).lean();
      } else {
        throw new Error('Either phoneNo or email must be provided');
      }

      if (!matchedUsers.length) {
        return result;
      }

      result.usersFound = matchedUsers.length;
      result.userIds = matchedUsers.map(u => u._id.toString());

      const userObjectIds = matchedUsers.map(u => u._id);

      const businessIds = matchedUsers
        .map(u => u.businessId)
        .filter(Boolean)
        .map(id => new Types.ObjectId(id));

      // -------------------------------
      // BACKUP USERS
      // -------------------------------
      if (matchedUsers.length) {
        const inserted = await backupConnection
          .collection('users_backup')
          .insertMany(matchedUsers, { ordered: false })
          .catch(() => ({ insertedCount: 0 }));

        result.usersBackedUp = inserted.insertedCount || 0;
      }

      // -------------------------------
      // BACKUP BUSINESSES
      // -------------------------------
      if (businessIds.length) {
        const businesses = await sourceBusinessModel
          .find({ _id: { $in: businessIds } })
          .lean();

        if (businesses.length) {
          const inserted = await backupConnection
            .collection('businessprofiles_backup')
            .insertMany(businesses as any[], { ordered: false })
            .catch(() => ({ insertedCount: 0 }));

          result.businessesBackedUp = inserted.insertedCount || 0;
        }
      }

      // -------------------------------
      // DELETE USERS (BULK)
      // -------------------------------
      const deleteUsersResult = await sourceUserModel.deleteMany({
        _id: { $in: userObjectIds },
      });

      result.usersDeleted = deleteUsersResult.deletedCount || 0;

      // -------------------------------
      // DELETE BUSINESSES (BULK)
      // -------------------------------
      if (businessIds.length) {
        const deleteBusinessResult = await sourceBusinessModel.deleteMany({
          _id: { $in: businessIds },
        });

        result.businessesDeleted = deleteBusinessResult.deletedCount || 0;
      }

      this.logger.log(
        `Deleted ${result.usersDeleted} users and ${result.businessesDeleted} businesses`,
      );

      return result;
    } catch (err: any) {
      this.logger.error(err.message);
      result.errors.push(`Database error: ${err.message}`);
      return result;
    }
  }

  // -------------------------------
  // DELETE ALL DOCUMENTS EXCEPT USERS AND BUSINESSES
  // -------------------------------
  async deleteAllDocumentsExceptUsersAndBusinesses(phoneNo?: string, email?: string): Promise<any> {
    const dbName = process.env.SANDBOX_DB || 'sandboxDb';
    const backupDbName = process.env.BACKUP_SANDBOX_DB || 'backUpSandboxDb';

    const result: any = {
      database: dbName,
      usersFound: 0,
      userIds: [],
      collections: {},
      errors: [],
    };

    try {
      const sourceConnection = this.connection.useDb(dbName, { useCache: true });
      const backupConnection = this.connection.useDb(backupDbName, { useCache: true });

      if (!sourceConnection.db || !backupConnection.db) {
        throw new Error('Database connections are not available');
      }

      const sourceUserModel = sourceConnection.model(User.name, UserSchema);

      // Find users
      let matchedUsers: any[] = [];
      if (phoneNo) {
        const norm = this.normalizePhone(phoneNo);
        matchedUsers = await sourceUserModel.find({ phoneNo: norm }).lean();
      } else if (email) {
        const normalizedEmail = email.trim().toLowerCase();
        matchedUsers = await sourceUserModel.find({
          $or: [
            { email: normalizedEmail },
            { workEmail: normalizedEmail },
          ],
        }).lean();
      } else {
        throw new Error('Either phoneNo or email must be provided');
      }

      if (!matchedUsers.length) {
        return result;
      }

      result.usersFound = matchedUsers.length;
      result.userIds = matchedUsers.map(u => u._id.toString());
      const userObjectIds = matchedUsers.map(u => u._id);

      // Collections with createdBy field
      const collectionsWithCreatedBy = [
        'businessprofile',
        'liveproducts',
        'salesproducts',
        'buyingrequests',
        'customers',
        'pepstaging_liveproducts',
        'rfqcarts',
        'selloffers',
        'leads',
      ];

      // Collections with user field
      const collectionsWithUser = ['new_catalog_schema', 'paymenthistories', 'emailremainders'];

      // Collections with phoneNo field (like otps)
      const collectionsWithPhoneNo = ['otps'];

      // Process each user
      for (const user of matchedUsers) {
        const userId = user._id.toString();
        const userObjectId = user._id;
        const phoneNumber = user.phoneNo;

        // Process collections with createdBy field
        for (const collectionName of collectionsWithCreatedBy) {
          try {
            if (!sourceConnection.db || !backupConnection.db) {
              throw new Error('Database connections are not available');
            }
            const query = { createdBy: userObjectId };
            const collection = sourceConnection.db.collection(collectionName);
            const documents = await collection.find(query).toArray();
            const found = documents.length;

            if (found > 0) {
              // Backup
              await this.ensureBackupCollection(backupConnection, collectionName);
              const backupCollectionName = `${collectionName}_backup`;
              const backupCollection = backupConnection.db.collection(backupCollectionName);
              await backupCollection.insertMany(documents as any[], { ordered: false }).catch(() => null);

              // Delete
              const deleteResult = await collection.deleteMany(query);
              const deleted = deleteResult.deletedCount || 0;

              if (!result.collections[collectionName]) {
                result.collections[collectionName] = { found: 0, backedUp: 0, deleted: 0 };
              }
              result.collections[collectionName].found += found;
              result.collections[collectionName].backedUp += found;
              result.collections[collectionName].deleted += deleted;
            }
          } catch (err: any) {
            this.logger.error(`Error processing collection ${collectionName} for user ${userId}: ${err.message}`);
            result.errors.push(`${collectionName}: ${err.message}`);
          }
        }

        // Process collections with user field
        for (const collectionName of collectionsWithUser) {
          try {
            if (!sourceConnection.db || !backupConnection.db) {
              throw new Error('Database connections are not available');
            }
            const query = { user: userObjectId };
            const collection = sourceConnection.db.collection(collectionName);
            const documents = await collection.find(query).toArray();
            const found = documents.length;

            if (found > 0) {
              // Backup
              await this.ensureBackupCollection(backupConnection, collectionName);
              const backupCollectionName = `${collectionName}_backup`;
              const backupCollection = backupConnection.db.collection(backupCollectionName);
              await backupCollection.insertMany(documents as any[], { ordered: false }).catch(() => null);

              // Delete
              const deleteResult = await collection.deleteMany(query);
              const deleted = deleteResult.deletedCount || 0;

              if (!result.collections[collectionName]) {
                result.collections[collectionName] = { found: 0, backedUp: 0, deleted: 0 };
              }
              result.collections[collectionName].found += found;
              result.collections[collectionName].backedUp += found;
              result.collections[collectionName].deleted += deleted;
            }
          } catch (err: any) {
            this.logger.error(`Error processing collection ${collectionName} for user ${userId}: ${err.message}`);
            result.errors.push(`${collectionName}: ${err.message}`);
          }
        }

        // Process collections with phoneNo field
        if (phoneNumber) {
          for (const collectionName of collectionsWithPhoneNo) {
            try {
              if (!sourceConnection.db || !backupConnection.db) {
                throw new Error('Database connections are not available');
              }
              const norm = this.normalizePhone(phoneNumber);
              const query = { phoneNo: norm };
              const collection = sourceConnection.db.collection(collectionName);
              const documents = await collection.find(query).toArray();
              const found = documents.length;

              if (found > 0) {
                // Backup
                await this.ensureBackupCollection(backupConnection, collectionName);
                const backupCollectionName = `${collectionName}_backup`;
                const backupCollection = backupConnection.db.collection(backupCollectionName);
                await backupCollection.insertMany(documents as any[], { ordered: false }).catch(() => null);

                // Delete
                const deleteResult = await collection.deleteMany(query);
                const deleted = deleteResult.deletedCount || 0;

                if (!result.collections[collectionName]) {
                  result.collections[collectionName] = { found: 0, backedUp: 0, deleted: 0 };
                }
                result.collections[collectionName].found += found;
                result.collections[collectionName].backedUp += found;
                result.collections[collectionName].deleted += deleted;
              }
            } catch (err: any) {
              this.logger.error(`Error processing collection ${collectionName} for phone ${phoneNumber}: ${err.message}`);
              result.errors.push(`${collectionName}: ${err.message}`);
            }
          }
        }
      }

      this.logger.log(`Completed deletion of related documents for ${result.userIds.length} users`);
      return result;
    } catch (err: any) {
      this.logger.error(`Error in deleteAllDocumentsExceptUsersAndBusinesses: ${err.message}`);
      result.errors.push(`Database error: ${err.message}`);
      return result;
    }
  }

  // Helper method to ensure backup collection exists
  private async ensureBackupCollection(backupConnection: Connection, collectionName: string): Promise<void> {
    if (!backupConnection.db) {
      throw new Error('Backup connection database is not available');
    }
    const db = backupConnection.db;
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);
    const backupCollectionName = `${collectionName}_backup`;

    if (!collectionNames.includes(backupCollectionName)) {
      await db.createCollection(backupCollectionName);
      this.logger.log(`Created backup collection ${backupCollectionName}`);
    }
  }
}