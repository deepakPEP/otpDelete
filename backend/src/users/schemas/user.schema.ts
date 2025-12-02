import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ collection: 'users', timestamps: true })
export class User extends Document {
  @Prop() com_id?: number;
  @Prop({ type: Types.ObjectId, ref: 'User' }) assignTo?: Types.ObjectId;
  @Prop({ type: Types.ObjectId, ref: 'Business' }) businessId?: Types.ObjectId;
  
  // Personal Information
  @Prop() name?: string;
  @Prop() firstName?: string;
  @Prop() lastName?: string;
  @Prop() middleName?: string;
  @Prop() jobTitle?: string;
  
  // Contact Information
  @Prop() email?: string;
  @Prop() workEmail?: string;
  @Prop() phoneNo?: string;
  @Prop() countryCode?: string;
  @Prop({
    type: {
      countryCode: String,
      number: String
    }
  }) workPhoneNo?: {
    countryCode: string;
    number: string;
  };

  // Business Information
  @Prop() businessLocation?: string;
  @Prop() businessType?: string;
  @Prop() userType?: string;
  @Prop({
    type: {
      street: String,
      city: String,
      state: String,
      zip: String
    }
  }) businessAddress?: {
    street: string;
    city: string;
    state: string;
    zip: string;
  };

  // Account Status
  @Prop({ default: false }) isBlocked?: boolean;
  @Prop({ default: false }) isDeleted?: boolean;
  @Prop({ default: false }) isEmailVerified?: boolean;
  @Prop({ default: false }) isPhoneNoVerified?: boolean;
  @Prop({ default: false }) isVerified?: boolean;
  @Prop({ default: false }) onBoardingComplete?: boolean;
  @Prop({ default: false }) onBoardingSkipped?: boolean;
  @Prop() status?: string;
  @Prop() source?: string;
  @Prop() accountId?: string;

  // Registration Status
  @Prop({
    type: {
      businessDetails: String,
      businessOperation: String,
      contactInformation: String,
      businessRepresentative: String
    }
  }) registerStatus?: {
    businessDetails: string;
    businessOperation: string;
    contactInformation: string;
    businessRepresentative: string;
  };

  @Prop({
    type: {
      businessDetails: String,
      businessRepresentative: String,
      businessOperation: String
    }
  }) unRegisterStatus?: {
    businessDetails: string;
    businessRepresentative: string;
    businessOperation: string;
  };

  // Buyer Stage
  @Prop({
    type: {
      businessOperation: String,
      sourcingDetails: String
    }
  }) buyerStage?: {
    businessOperation: string;
    sourcingDetails: string;
  };

  // Current Plan
  @Prop({
    type: {
      planId: String,
      billingType: String,
      startDate: Date,
      endDate: Date,
      isActive: Boolean,
      isTrial: Boolean,
      paymentRefId: String
    }
  }) currentPlan?: {
    planId: string;
    billingType: string;
    startDate: Date;
    endDate: Date;
    isActive: boolean;
    isTrial: boolean;
    paymentRefId: string;
  };

  // Counters
  @Prop({
    type: {
      rfqIds: [String],
      emailMarketing: Number,
      socialPostsUsed: [String],
      aiProductsUsed: Number,
      quotesGenerated: [String],
      sellOffer: [String],
      liveChatsInitiated: [String],
      businessProfileViews: [String],
      lastReset: Date
    }
  }) counters?: {
    rfqIds: string[];
    emailMarketing: number;
    socialPostsUsed: string[];
    aiProductsUsed: number;
    quotesGenerated: string[];
    sellOffer: string[];
    liveChatsInitiated: string[];
    businessProfileViews: string[];
    lastReset: Date;
  };
}

export const UserSchema = SchemaFactory.createForClass(User);
