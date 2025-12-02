import React, { useState } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Alert,
  Snackbar,
  InputAdornment,
  IconButton,
  Tooltip,
} from '@mui/material';
import { VpnKey, ContentCopy, CheckCircle } from '@mui/icons-material';
import ApiService from '@/services/api';
import type { OtpResponse } from '@/types';

export default function OtpLookup() {
  const [phone, setPhone] = useState('');
  const [otpResult, setOtpResult] = useState<OtpResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!phone.trim()) {
      setError('Please enter a phone number');
      return;
    }

    setLoading(true);
    setError(null);
    setHasSearched(true);
    setOtpResult(null);

    try {
      const response = await ApiService.findOtp(phone.trim());
      setOtpResult(response);
    } catch (err: any) {
      if (err.response?.status === 404) {
        setError(`No OTP found for phone number "${phone}"`);
      } else {
        setError(err instanceof Error ? err.message : 'OTP lookup failed');
      }
      setOtpResult(null);
    } finally {
      setLoading(false);
    }
  };

  const handleCopyOtp = async () => {
    if (otpResult?.otp) {
      try {
        await navigator.clipboard.writeText(otpResult.otp);
        setCopySuccess(true);
        setTimeout(() => setCopySuccess(false), 2000);
      } catch (err) {
        setError('Failed to copy OTP to clipboard');
      }
    }
  };

  const handleCloseSnackbar = () => {
    setError(null);
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        OTP Lookup
      </Typography>
      
      <Typography variant="body1" color="text.secondary" sx={{ mb: 4 }}>
        Find the latest One-Time Password (OTP) for any phone number in the system.
      </Typography>

      <Paper sx={{ p: 3, mb: 4 }}>
        <Box component="form" onSubmit={handleSearch}>
          <TextField
            fullWidth
            label="Phone Number"
            value={phone}
            onChange={(e) => setPhone(e.target.value)}
            placeholder="Enter phone number (e.g., 1234567890)"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <VpnKey />
                </InputAdornment>
              ),
            }}
            sx={{ mb: 3 }}
          />
          
          <Button
            type="submit"
            variant="contained"
            disabled={loading || !phone.trim()}
            startIcon={<VpnKey />}
          >
            {loading ? 'Searching...' : 'Find OTP'}
          </Button>
        </Box>
      </Paper>

      {hasSearched && (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            OTP Lookup Results
          </Typography>
          
          {otpResult ? (
            <Box>
              <Alert severity="success" sx={{ mb: 3 }}>
                OTP found for phone number: <strong>{otpResult.phoneNo}</strong>
              </Alert>
              
              <Paper 
                sx={{ 
                  p: 3, 
                  textAlign: 'center', 
                  backgroundColor: 'success.light',
                  color: 'success.contrastText',
                  border: '2px solid',
                  borderColor: 'success.main'
                }}
              >
                <Typography variant="body2" sx={{ mb: 1, opacity: 0.8 }}>
                  One-Time Password
                </Typography>
                <Typography 
                  variant="h3" 
                  component="div" 
                  sx={{ 
                    fontFamily: 'monospace',
                    letterSpacing: '0.2em',
                    mb: 2,
                    fontWeight: 'bold'
                  }}
                >
                  {otpResult.otp}
                </Typography>
                
                <Tooltip title={copySuccess ? "Copied!" : "Copy to clipboard"}>
                  <IconButton
                    onClick={handleCopyOtp}
                    sx={{ 
                      color: 'success.contrastText',
                      backgroundColor: 'rgba(255,255,255,0.1)',
                      '&:hover': {
                        backgroundColor: 'rgba(255,255,255,0.2)',
                      }
                    }}
                  >
                    {copySuccess ? <CheckCircle /> : <ContentCopy />}
                  </IconButton>
                </Tooltip>
              </Paper>
            </Box>
          ) : error ? (
            <Alert severity="error">
              {error}
            </Alert>
          ) : null}
        </Paper>
      )}

      <Snackbar
        open={!!error}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
      >
        <Alert onClose={handleCloseSnackbar} severity="error">
          {error}
        </Alert>
      </Snackbar>

      <Snackbar
        open={copySuccess}
        autoHideDuration={2000}
        onClose={() => setCopySuccess(false)}
      >
        <Alert severity="success">
          OTP copied to clipboard!
        </Alert>
      </Snackbar>
    </Box>
  );
}