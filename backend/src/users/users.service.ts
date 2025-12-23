import { Injectable, Logger } from '@nestjs/common';
import { InjectModel, InjectConnection } from '@nestjs/mongoose';
import { Model, Types, ClientSession, Connection, Schema } from 'mongoose';
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

  async findByEmail(email: string) {
    if (!email) return [];
    const normalizedEmail = email.trim().toLowerCase();
    return this.userModel.find({
      $or: [
        { email: { $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
        { workEmail: { $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
      ],
    }).lean();
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

  // Backup methods
  async ensureBackupDatabase(backupDbName: string): Promise<Connection> {
    // Create connection to backup database (uses same MongoDB connection, different database)
    const backupConnection = this.connection.useDb(backupDbName, { useCache: true });
    
    // Ensure collections exist by creating them if they don't
    if (!backupConnection.db) {
      throw new Error(`Backup database connection failed for ${backupDbName}`);
    }
    const db = backupConnection.db;
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);

    if (!collectionNames.includes('users_backup')) {
      await db.createCollection('users_backup');
      this.logger.log(`Created collection users_backup in ${backupDbName}`);
    }
    if (!collectionNames.includes('businessprofiles_backup')) {
      await db.createCollection('businessprofiles_backup');
      this.logger.log(`Created collection businessprofiles_backup in ${backupDbName}`);
    }
    if (!collectionNames.includes('otps_backup')) {
      await db.createCollection('otps_backup');
      this.logger.log(`Created collection otps_backup in ${backupDbName}`);
    }

    return backupConnection;
  }

  async backupUsers(users: any[], backupConnection: Connection): Promise<number> {
    if (!users || users.length === 0) return 0;
    if (!backupConnection.db) {
      this.logger.warn('Backup connection database is not available');
      return 0;
    }
    const backupCollection = backupConnection.db.collection('users_backup');
    const result = await backupCollection.insertMany(users, { ordered: false }).catch(err => {
      // Handle duplicate key errors gracefully
      if (err.code === 11000) {
        this.logger.warn('Some users already exist in backup, skipping duplicates');
        return { insertedCount: 0 };
      }
      throw err;
    });
    return result.insertedCount || 0;
  }

  async backupBusinesses(businesses: any[], backupConnection: Connection): Promise<number> {
    if (!businesses || businesses.length === 0) return 0;
    if (!backupConnection.db) {
      this.logger.warn('Backup connection database is not available');
      return 0;
    }
    const backupCollection = backupConnection.db.collection('businessprofiles_backup');
    const result = await backupCollection.insertMany(businesses, { ordered: false }).catch(err => {
      if (err.code === 11000) {
        this.logger.warn('Some businesses already exist in backup, skipping duplicates');
        return { insertedCount: 0 };
      }
      throw err;
    });
    return result.insertedCount || 0;
  }

  async backupOtpsForUsers(userIds: string[], sourceConnection: Connection, backupConnection: Connection): Promise<number> {
    if (!userIds || userIds.length === 0) return 0;
    
    // Get phone numbers from users
    const userModel = sourceConnection.model(User.name);
    const users = await userModel.find({ _id: { $in: userIds.map(id => new Types.ObjectId(id)) } }).lean();
    const phoneNumbers = users.map(u => u.phoneNo).filter(Boolean);
    
    if (phoneNumbers.length === 0) return 0;

    // Get OTPs for these phone numbers
    // Note: OTP model might not be registered in this connection, so we'll query directly
    if (!sourceConnection.db) {
      this.logger.warn('Source connection database is not available');
      return 0;
    }
    const otpsCollection = sourceConnection.db.collection('otps');
    const otps = await otpsCollection.find({ phoneNo: { $in: phoneNumbers } }).toArray();
    
    if (otps.length === 0) return 0;

    if (!backupConnection.db) {
      this.logger.warn('Backup connection database is not available');
      return 0;
    }
    const backupCollection = backupConnection.db.collection('otps_backup');
    const result = await backupCollection.insertMany(otps, { ordered: false }).catch(err => {
      if (err.code === 11000) {
        this.logger.warn('Some OTPs already exist in backup, skipping duplicates');
        return { insertedCount: 0 };
      }
      throw err;
    });
    return result.insertedCount || 0;
  }

  async deleteByEmailWithBackup(email: string) {
    // Get source databases from environment variable (comma-separated)
    const sourceDbsEnv = process.env.SOURCE_DATABASES || 'sandboxDb,pepagoraDb';
    const sourceDatabases = sourceDbsEnv.split(',').map(db => db.trim()).filter(Boolean);
    
    // Get backup database names from environment variable (comma-separated, same order as SOURCE_DATABASES)
    // If not provided, auto-generate using prefix
    const backupDbsEnv = process.env.BACKUP_DATABASES;
    const backupPrefix = process.env.BACKUP_DB_PREFIX || 'backup';
    
    // Build database configuration
    const databases = sourceDatabases.map((dbName, index) => {
      let backupName: string;
      if (backupDbsEnv) {
        const backupDbs = backupDbsEnv.split(',').map(db => db.trim()).filter(Boolean);
        backupName = backupDbs[index] || `${backupPrefix}${dbName.charAt(0).toUpperCase() + dbName.slice(1)}`;
      } else {
        // Auto-generate backup name with prefix
        backupName = `${backupPrefix}${dbName.charAt(0).toUpperCase() + dbName.slice(1)}`;
      }
      return { name: dbName, backupName };
    });

    // Initialize results object dynamically
    const results: any = {};
    sourceDatabases.forEach(dbName => {
      results[dbName] = {
        usersFound: 0,
        usersBackedUp: 0,
        usersDeleted: 0,
        businessesBackedUp: 0,
        businessesDeleted: 0,
        otpsBackedUp: 0,
        errors: [],
      };
    });

    for (const dbConfig of databases) {
      try {
        // Connect to source database
        const sourceConnection = this.connection.useDb(dbConfig.name, { useCache: true });
        const sourceUserModel = sourceConnection.model(User.name, UserSchema);
        const sourceBusinessModel = sourceConnection.model(Business.name, BusinessSchema);

        // Find users by email
        const normalizedEmail = email.trim().toLowerCase();
        const matchedUsers = await sourceUserModel.find({
          $or: [
            { email: { $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
            { workEmail: { $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
          ],
        }).lean();

        if (!matchedUsers || matchedUsers.length === 0) {
          this.logger.log(`No users found in ${dbConfig.name} for email: ${email}`);
          continue;
        }

        results[dbConfig.name].usersFound = matchedUsers.length;
        const userIds = matchedUsers.map(u => u._id.toString());
        const businessIds = matchedUsers
          .map(u => u.businessId ? u.businessId.toString() : null)
          .filter((id): id is string => !!id);

        // Get businesses to backup
        let businessesToBackup: any[] = [];
        if (businessIds.length > 0) {
          const objectIds = businessIds.map(id => new Types.ObjectId(id));
          businessesToBackup = await sourceBusinessModel.find({ _id: { $in: objectIds } }).lean();
        }

        // Create backup database connection
        const backupConnection = await this.ensureBackupDatabase(dbConfig.backupName);

        // Backup users
        const usersBackedUp = await this.backupUsers(matchedUsers, backupConnection);
        results[dbConfig.name].usersBackedUp = usersBackedUp;
        this.logger.log(`Backed up ${usersBackedUp} users to ${dbConfig.backupName}`);

        // Backup businesses
        const businessesBackedUp = await this.backupBusinesses(businessesToBackup, backupConnection);
        results[dbConfig.name].businessesBackedUp = businessesBackedUp;
        this.logger.log(`Backed up ${businessesBackedUp} businesses to ${dbConfig.backupName}`);

        // Backup OTPs
        const otpsBackedUp = await this.backupOtpsForUsers(userIds, sourceConnection, backupConnection);
        results[dbConfig.name].otpsBackedUp = otpsBackedUp;
        this.logger.log(`Backed up ${otpsBackedUp} OTPs to ${dbConfig.backupName}`);

        // Now delete users and businesses
        const session = await sourceConnection.startSession();
        try {
          await session.withTransaction(async () => {
            for (const user of matchedUsers) {
              try {
                const businessId = user.businessId ? user.businessId.toString() : null;

                // Delete user
                const userDeleteResult = await sourceUserModel.deleteOne({ _id: user._id }).session(session);
                results[dbConfig.name].usersDeleted += userDeleteResult.deletedCount || 0;

                // Check and delete business if no other users reference it
                if (businessId) {
                  const otherUsersCount = await sourceUserModel.countDocuments({
                    businessId: user.businessId,
                    _id: { $ne: user._id },
                  }).session(session);

                  if (otherUsersCount === 0) {
                    const businessDeleteResult = await sourceBusinessModel.deleteOne({
                      _id: user.businessId,
                    }).session(session);
                    results[dbConfig.name].businessesDeleted += businessDeleteResult.deletedCount || 0;
                    this.logger.log(`Business ${businessId} deleted from ${dbConfig.name}`);
                  } else {
                    this.logger.log(`Business ${businessId} not deleted - still referenced by ${otherUsersCount} other user(s) in ${dbConfig.name}`);
                  }
                }
              } catch (err: any) {
                this.logger.error(`Error deleting user ${user._id} in ${dbConfig.name}: ${err.message}`);
                results[dbConfig.name].errors.push(`User ${user._id}: ${err.message}`);
              }
            }
          });
        } catch (err: any) {
          this.logger.error(`Transaction error in ${dbConfig.name}: ${err.message}`);
          results[dbConfig.name].errors.push(`Transaction error: ${err.message}`);
        } finally {
          session.endSession();
        }

        this.logger.log(`Completed deletion in ${dbConfig.name}: ${results[dbConfig.name].usersDeleted} users, ${results[dbConfig.name].businessesDeleted} businesses deleted`);
      } catch (err: any) {
        this.logger.error(`Error processing ${dbConfig.name}: ${err.message}`);
        results[dbConfig.name].errors.push(`Database error: ${err.message}`);
      }
    }

    return results;
  }

  // Helper method to process deletion for a single database
  async processDeletionForSingleDb(
    matchedUsers: any[],
    dbName: string,
    backupDbName: string,
  ): Promise<any> {
    const result: any = {
      database: dbName,
      usersFound: 0,
      usersBackedUp: 0,
      usersDeleted: 0,
      businessesBackedUp: 0,
      businessesDeleted: 0,
      otpsBackedUp: 0,
      errors: [],
    };

    try {
      if (!matchedUsers || matchedUsers.length === 0) {
        return result;
      }

      // Connect to source database
      const sourceConnection = this.connection.useDb(dbName, { useCache: true });
      const sourceUserModel = sourceConnection.model(User.name, UserSchema);
      const sourceBusinessModel = sourceConnection.model(Business.name, BusinessSchema);

      result.usersFound = matchedUsers.length;
      const userIds = matchedUsers.map(u => u._id.toString());
      const businessIds = matchedUsers
        .map(u => (u.businessId ? u.businessId.toString() : null))
        .filter((id): id is string => !!id);

      // Get businesses to backup
      let businessesToBackup: any[] = [];
      if (businessIds.length > 0) {
        const objectIds = businessIds.map(id => new Types.ObjectId(id));
        businessesToBackup = await sourceBusinessModel.find({ _id: { $in: objectIds } }).lean();
      }

      // Create backup database connection
      const backupConnection = await this.ensureBackupDatabase(backupDbName);

      // Backup users
      const usersBackedUp = await this.backupUsers(matchedUsers, backupConnection);
      result.usersBackedUp = usersBackedUp;
      this.logger.log(`Backed up ${usersBackedUp} users to ${backupDbName}`);

      // Backup businesses
      const businessesBackedUp = await this.backupBusinesses(businessesToBackup, backupConnection);
      result.businessesBackedUp = businessesBackedUp;
      this.logger.log(`Backed up ${businessesBackedUp} businesses to ${backupDbName}`);

      // Backup OTPs
      const otpsBackedUp = await this.backupOtpsForUsers(userIds, sourceConnection, backupConnection);
      result.otpsBackedUp = otpsBackedUp;
      this.logger.log(`Backed up ${otpsBackedUp} OTPs to ${backupDbName}`);

      // Now delete users and businesses
      const session = await sourceConnection.startSession();
      try {
        await session.withTransaction(async () => {
          for (const user of matchedUsers) {
            try {
              const businessId = user.businessId ? user.businessId.toString() : null;

              // Delete user
              const userDeleteResult = await sourceUserModel.deleteOne({ _id: user._id }).session(session);
              result.usersDeleted += userDeleteResult.deletedCount || 0;

              // Check and delete business if no other users reference it
              if (businessId) {
                const otherUsersCount = await sourceUserModel.countDocuments({
                  businessId: user.businessId,
                  _id: { $ne: user._id },
                }).session(session);

                if (otherUsersCount === 0) {
                  const businessDeleteResult = await sourceBusinessModel.deleteOne({
                    _id: user.businessId,
                  }).session(session);
                  result.businessesDeleted += businessDeleteResult.deletedCount || 0;
                  this.logger.log(`Business ${businessId} deleted from ${dbName}`);
                } else {
                  this.logger.log(
                    `Business ${businessId} not deleted - still referenced by ${otherUsersCount} other user(s) in ${dbName}`,
                  );
                }
              }
            } catch (err: any) {
              this.logger.error(`Error deleting user ${user._id} in ${dbName}: ${err.message}`);
              result.errors.push(`User ${user._id}: ${err.message}`);
            }
          }
        });
      } catch (err: any) {
        this.logger.error(`Transaction error in ${dbName}: ${err.message}`);
        result.errors.push(`Transaction error: ${err.message}`);
      } finally {
        session.endSession();
      }

      this.logger.log(
        `Completed deletion in ${dbName}: ${result.usersDeleted} users, ${result.businessesDeleted} businesses deleted`,
      );
    } catch (err: any) {
      this.logger.error(`Error processing ${dbName}: ${err.message}`);
      result.errors.push(`Database error: ${err.message}`);
    }

    return result;
  }

  // Delete by phone from a specific database
  async deleteByPhoneFromDb(phoneNo: string, dbName: string, backupDbName: string): Promise<any> {
    try {
      // Connect to source database
      const sourceConnection = this.connection.useDb(dbName, { useCache: true });
      const sourceUserModel = sourceConnection.model(User.name, UserSchema);

      // Find users by phone
      const norm = this.normalizePhone(phoneNo);
      const matchedUsers = await sourceUserModel.find({ phoneNo: norm }).lean();

      if (!matchedUsers || matchedUsers.length === 0) {
        this.logger.log(`No users found in ${dbName} for phone: ${phoneNo}`);
        return {
          database: dbName,
          usersFound: 0,
          usersBackedUp: 0,
          usersDeleted: 0,
          businessesBackedUp: 0,
          businessesDeleted: 0,
          otpsBackedUp: 0,
          errors: [],
        };
      }

      // Process deletion
      return await this.processDeletionForSingleDb(matchedUsers, dbName, backupDbName);
    } catch (err: any) {
      this.logger.error(`Error in deleteByPhoneFromDb for ${dbName}: ${err.message}`);
      return {
        database: dbName,
        usersFound: 0,
        usersBackedUp: 0,
        usersDeleted: 0,
        businessesBackedUp: 0,
        businessesDeleted: 0,
        otpsBackedUp: 0,
        errors: [`Database error: ${err.message}`],
      };
    }
  }

  // Delete by email from a specific database
  async deleteByEmailFromDb(email: string, dbName: string, backupDbName: string): Promise<any> {
    try {
      // Connect to source database
      const sourceConnection = this.connection.useDb(dbName, { useCache: true });
      const sourceUserModel = sourceConnection.model(User.name, UserSchema);

      // Find users by email
      const normalizedEmail = email.trim().toLowerCase();
      const matchedUsers = await sourceUserModel
        .find({
          $or: [
            { email: { $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
            {
              workEmail: {
                $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i'),
              },
            },
          ],
        })
        .lean();

      if (!matchedUsers || matchedUsers.length === 0) {
        this.logger.log(`No users found in ${dbName} for email: ${email}`);
        return {
          database: dbName,
          usersFound: 0,
          usersBackedUp: 0,
          usersDeleted: 0,
          businessesBackedUp: 0,
          businessesDeleted: 0,
          otpsBackedUp: 0,
          errors: [],
        };
      }

      // Process deletion
      return await this.processDeletionForSingleDb(matchedUsers, dbName, backupDbName);
    } catch (err: any) {
      this.logger.error(`Error in deleteByEmailFromDb for ${dbName}: ${err.message}`);
      return {
        database: dbName,
        usersFound: 0,
        usersBackedUp: 0,
        usersDeleted: 0,
        businessesBackedUp: 0,
        businessesDeleted: 0,
        otpsBackedUp: 0,
        errors: [`Database error: ${err.message}`],
      };
    }
  }

  // Helper method to ensure backup collection exists
  async ensureBackupCollection(backupConnection: Connection, collectionName: string): Promise<void> {
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

  // Helper method to backup and delete collection documents
  async processCollectionDocuments(
    sourceConnection: Connection,
    backupConnection: Connection,
    collectionName: string,
    query: any,
    userId: string,
  ): Promise<{ found: number; backedUp: number; deleted: number; error?: string }> {
    try {
      if (!sourceConnection.db || !backupConnection.db) {
        throw new Error('Database connection not available');
      }

      // Find documents
      const collection = sourceConnection.db.collection(collectionName);
      const documents = await collection.find(query).toArray();
      const found = documents.length;

      if (found === 0) {
        return { found: 0, backedUp: 0, deleted: 0 };
      }

      // Ensure backup collection exists
      await this.ensureBackupCollection(backupConnection, collectionName);

      // Backup documents
      const backupCollectionName = `${collectionName}_backup`;
      const backupCollection = backupConnection.db.collection(backupCollectionName);
      let backedUp = 0;
      if (documents.length > 0) {
        const backupResult = await backupCollection.insertMany(documents, { ordered: false }).catch(err => {
          if (err.code === 11000) {
            this.logger.warn(`Some documents already exist in ${backupCollectionName}, skipping duplicates`);
            return { insertedCount: 0 };
          }
          throw err;
        });
        backedUp = backupResult.insertedCount || 0;
      }

      // Delete documents
      const deleteResult = await collection.deleteMany(query);
      const deleted = deleteResult.deletedCount || 0;

      this.logger.log(
        `Collection ${collectionName} for user ${userId}: Found ${found}, Backed up ${backedUp}, Deleted ${deleted}`,
      );

      return { found, backedUp, deleted };
    } catch (err: any) {
      this.logger.error(`Error processing collection ${collectionName} for user ${userId}: ${err.message}`);
      return { found: 0, backedUp: 0, deleted: 0, error: err.message };
    }
  }

  // Main method to delete user-related data from a specific database
  async deleteUserRelatedDataFromDb(
    dbName: string,
    backupDbName: string,
    phoneNo?: string,
    email?: string,
  ): Promise<any> {
    const result: any = {
      database: dbName,
      usersFound: 0,
      userIds: [],
      collections: {},
      errors: [],
    };

    try {
      // Connect to source database
      const sourceConnection = this.connection.useDb(dbName, { useCache: true });
      const sourceUserModel = sourceConnection.model(User.name, UserSchema);

      // Find users
      let matchedUsers: any[] = [];
      if (phoneNo) {
        const norm = this.normalizePhone(phoneNo);
        matchedUsers = await sourceUserModel.find({ phoneNo: norm }).lean();
      } else if (email) {
        const normalizedEmail = email.trim().toLowerCase();
        matchedUsers = await sourceUserModel
          .find({
            $or: [
              { email: { $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
              {
                workEmail: {
                  $regex: new RegExp(`^${normalizedEmail.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i'),
                },
              },
            ],
          })
          .lean();
      } else {
        throw new Error('Either phoneNo or email must be provided');
      }

      if (!matchedUsers || matchedUsers.length === 0) {
        this.logger.log(`No users found in ${dbName} for phoneNo: ${phoneNo || 'N/A'}, email: ${email || 'N/A'}`);
        return result;
      }

      result.usersFound = matchedUsers.length;
      result.userIds = matchedUsers.map((u: any) => u._id.toString());

      // Ensure backup database exists
      const backupConnection = await this.ensureBackupDatabase(backupDbName);

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

      // Process each user
      for (const user of matchedUsers) {
        const userId = user._id.toString();
        const userObjectId = user._id;

        // Process collections with createdBy field
        for (const collectionName of collectionsWithCreatedBy) {
          const query = { createdBy: userObjectId };
          const collectionResult = await this.processCollectionDocuments(
            sourceConnection,
            backupConnection,
            collectionName,
            query,
            userId,
          );

          if (!result.collections[collectionName]) {
            result.collections[collectionName] = { found: 0, backedUp: 0, deleted: 0 };
          }
          result.collections[collectionName].found += collectionResult.found;
          result.collections[collectionName].backedUp += collectionResult.backedUp;
          result.collections[collectionName].deleted += collectionResult.deleted;
          if (collectionResult.error) {
            result.errors.push(`${collectionName}: ${collectionResult.error}`);
          }
        }

        // Process collections with user field
        for (const collectionName of collectionsWithUser) {
          const query = { user: userObjectId };
          const collectionResult = await this.processCollectionDocuments(
            sourceConnection,
            backupConnection,
            collectionName,
            query,
            userId,
          );

          if (!result.collections[collectionName]) {
            result.collections[collectionName] = { found: 0, backedUp: 0, deleted: 0 };
          }
          result.collections[collectionName].found += collectionResult.found;
          result.collections[collectionName].backedUp += collectionResult.backedUp;
          result.collections[collectionName].deleted += collectionResult.deleted;
          if (collectionResult.error) {
            result.errors.push(`${collectionName}: ${collectionResult.error}`);
          }
        }
      }

      this.logger.log(`Completed deletion in ${dbName}: ${result.userIds.length} users processed`);
    } catch (err: any) {
      this.logger.error(`Error processing ${dbName}: ${err.message}`);
      result.errors.push(`Database error: ${err.message}`);
    }

    return result;
  }
}
