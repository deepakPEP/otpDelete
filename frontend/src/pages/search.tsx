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
} from '@mui/material';
import { Search as SearchIcon } from '@mui/icons-material';
import UserTable from '@/components/UserTable';
import ApiService from '@/services/api';
import type { SearchMatch } from '@/types';

export default function Search() {
  const [phone, setPhone] = useState('');
  const [searchResults, setSearchResults] = useState<SearchMatch[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!phone.trim()) {
      setError('Please enter a phone number');
      return;
    }

    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      const response = await ApiService.searchByPhone(phone.trim());
      setSearchResults(response.matched);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Search failed');
      setSearchResults([]);
    } finally {
      setLoading(false);
    }
  };

  const handleCloseSnackbar = () => {
    setError(null);
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Search Users
      </Typography>
      
      <Typography variant="body1" color="text.secondary" sx={{ mb: 4 }}>
        Search for users by their phone number to view their details and associated business information.
      </Typography>

      <Paper sx={{ p: 3, mb: 4 }}>
        <Box component="form" onSubmit={handleSearch}>
          <TextField
            fullWidth
            label="Phone Number"
            value={phone}
            onChange={(e) => setPhone(e.target.value)}
            placeholder="Enter phone number (e.g., +1234567890)"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              ),
            }}
            sx={{ mb: 3 }}
          />
          
          <Button
            type="submit"
            variant="contained"
            disabled={loading || !phone.trim()}
            startIcon={<SearchIcon />}
          >
            {loading ? 'Searching...' : 'Search'}
          </Button>
        </Box>
      </Paper>

      {hasSearched && (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Search Results
            {searchResults.length > 0 && ` (${searchResults.length})`}
          </Typography>
          
          {searchResults.length === 0 ? (
            <Alert severity="info">
                No users found with phone number &quot;{phone}&quot;
            </Alert>
          ) : (
            <UserTable users={searchResults} />
          )}
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
    </Box>
  );
}