import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true }) 
export class Otp extends Document {
  @Prop({ required: true })
  otp: string;

  @Prop({ required: true })
  phoneNo: string;

  @Prop()
  countryCode: string;

  @Prop()
  expiry_date: Date;

  @Prop({ default: false })
  verified: boolean;

  @Prop({ default: 0 })
  retryCount: number;

  @Prop()
  createdAt: Date;

  @Prop()
  updatedAt: Date;
}

export const OtpSchema = SchemaFactory.createForClass(Otp);