import axios from 'axios';
import type { UploadResponse, DeleteResponse, SearchResponse, OtpResponse } from '@/types';

// Use relative URL in production, localhost in development
const API_BASE_URL = process.env.NODE_ENV === 'production' 
  ? process.env.NEXT_PUBLIC_API_URL || `${window.location.protocol}//${window.location.hostname}:4000`
  : process.env.NEXT_PUBLIC_API_URL || 'http://localhost:4000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export class ApiService {
  // Upload phone file (CSV or JSON)
  static async uploadPhoneFile(file: File): Promise<UploadResponse> {
    const formData = new FormData();

    formData.append('file', file);
    
    const response = await api.post('/api/upload-phonefile', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    
    return response.data;
  }

  // Delete selected users by IDs
  static async deleteSelectedUsers(userIds: string[]): Promise<DeleteResponse> {
    const response = await api.post('/api/delete-selected', { userIds });
    return response.data;
  }

  // Delete users by phone number
  static async deleteByPhone(phoneNo: string): Promise<DeleteResponse> {
    const response = await api.post('/api/delete-by-phone', { phoneNo });
    return response.data;
  }

  // Search users by phone number
  static async searchByPhone(phone: string): Promise<SearchResponse> {
    const response = await api.get(`/api/matches?phone=${encodeURIComponent(phone)}`);
    return response.data;
  }

  // Delete all users from uploaded file
  static async deleteAllFromFile(phoneNumbers: string[]): Promise<DeleteResponse> {
    const response = await api.post('/api/delete-all-from-file', { phoneNumbers });
    return response.data;
  }

  // Find OTP by phone number
  static async findOtp(phoneNo: string): Promise<OtpResponse> {
    const response = await api.post('/findOtp', { phoneNo });
    return response.data;
  }
}

export default ApiService;