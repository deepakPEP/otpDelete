import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ collection: 'businessprofiles', timestamps: true })
export class Business extends Document {
  @Prop() com_id?: number;
  @Prop({ type: Types.ObjectId, ref: 'User' }) createdBy?: Types.ObjectId;
  @Prop({ type: Types.ObjectId, ref: 'User' }) updatedBy?: Types.ObjectId;
  @Prop() businessName?: string;
  @Prop() legalOwnerName?: string;
  @Prop() legalBusinessName?: string;
  @Prop() businessType?: string;
  @Prop() yearOfEstablishment?: number;
  @Prop() noOfEmployees?: string;
  @Prop([String]) businessTypeSpecific?: string[];
  @Prop() supplierBadge?: string;
  
  @Prop({
    type: {
      addressLine: String,
      city: String,
      state: String,
      pincode: String,
      country: {
        name: String,
        code: String
      }
    }
  }) shippingAddress?: {
    addressLine: string;
    city: string;
    state: string;
    pincode: string;
    country: {
      name: string;
      code: string;
    };
  };

  @Prop({
    type: {
      addressLine: String,
      city: String,
      state: String,
      pincode: String,
      country: {
        name: String,
        code: String
      }
    }
  }) businessAddress?: {
    addressLine: string;
    city: string;
    state: string;
    pincode: string;
    country: {
      name: string;
      code: string;
    };
  };

  @Prop({
    type: {
      name: String,
      code: String
    }
  }) businessLocation?: {
    name: string;
    code: string;
  };

  @Prop({
    type: {
      countryCode: String,
      number: String
    }
  }) businessPhoneNo?: {
    countryCode: string;
    number: string;
  };

  @Prop() businessEmail?: string;
  @Prop([String]) mainProducts?: string[];
  @Prop() totalFactorySize?: string;
  @Prop() noOfProductionLines?: number;
  @Prop() annualOutputValue?: string;
  @Prop() productionFacilities?: string;
  @Prop([String]) certificates?: string[];
  @Prop([String]) mainMarkets?: string[];
  @Prop([String]) shippingMethod?: string[];
  @Prop([String]) acceptedCurrency?: string[];
  @Prop([String]) paymentMethods?: string[];
  @Prop() otherPaymentMethod?: string;
  @Prop() paymentTerms?: string;
  @Prop([String]) languageSpoken?: string[];
  @Prop() nearestPort?: string;
  
  @Prop({
    type: {
      value: Number,
      unit: String
    }
  }) averageLeadTime?: {
    value: number;
    unit: string;
  };

  @Prop({
    type: {
      currency: {
        code: String,
        name: String,
        symbol: String
      },
      value: Number
    }
  }) minOrderValue?: {
    currency: {
      code: string;
      name: string;
      symbol: string;
    };
    value: number;
  };

  @Prop([String]) deliveryTerm?: string[];
  @Prop() kycStatus?: string;
  @Prop() kycReferenceId?: string;
  @Prop() IdentityVerifyStatus?: string;
  @Prop() BusinessVerifyStatus?: string;
  @Prop([String]) businessTaxInfo?: string[];
  @Prop() timeZone?: string;

  @Prop({
    type: {
      BusinessDetails: String,
      CompanyRegistrationDetails: String,
      Additional: String,
      FactoryWarehouseDetails: String,
      BrandingMedia: String,
      MarketLogistics: String,
      ShippingPaymentTerms: String,
      AdditionalTradeDetails: String
    }
  }) stageStatus?: {
    BusinessDetails: string;
    CompanyRegistrationDetails: string;
    Additional: string;
    FactoryWarehouseDetails: string;
    BrandingMedia: string;
    MarketLogistics: string;
    ShippingPaymentTerms: string;
    AdditionalTradeDetails: string;
  };

  @Prop({
    type: {
      shareWithTrustedPartners: Boolean,
      dataForAnalytics: Boolean,
      consentToProcessData: Boolean
    }
  }) dataPrivacySettings?: {
    shareWithTrustedPartners: boolean;
    dataForAnalytics: boolean;
    consentToProcessData: boolean;
  };

  @Prop({
    type: {
      _id: Types.ObjectId,
      name: String,
      liveUrl: String,
      uniqueId: String
    }
  }) industry?: {
    _id: Types.ObjectId;
    name: string;
    liveUrl: string;
    uniqueId: string;
  };
}

export const BusinessSchema = SchemaFactory.createForClass(Business);
