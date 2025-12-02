// Types that match the backend API responses

export interface MatchedUser {
  userId: string;
  name: string;
  phoneNo: string;
  businessId: string | null;
  businessExists: boolean;
}

export interface UploadResponse {
  matched: MatchedUser[];
}

export interface DeleteResult {
  userId: string;
  businessId?: string | null;
  userDeleted: boolean;
  businessDeleted: boolean;
  error?: string;
}

export interface DeleteResponse {
  results: DeleteResult[];
  message?: string;
}

export interface SearchMatch {
  userId: string;
  name: string;
  phoneNo: string;
  businessId: string | null;
}

export interface SearchResponse {
  matched: SearchMatch[];
}

// OTP related types
export interface OtpRequest {
  phoneNo: string;
}

export interface OtpResponse {
  phoneNo: string;
  otp: string;
}