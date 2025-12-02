import React, { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  Snackbar,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Chip,
} from '@mui/material';
import {
  Delete as DeleteIcon,
  Phone as PhoneIcon,
} from '@mui/icons-material';
import UserTable from '@/components/UserTable';
import ApiService from '@/services/api';
import type { MatchedUser, DeleteResult } from '@/types';

export default function Manage() {
  const [selectedUsers, setSelectedUsers] = useState<string[]>([]);
  const [deletePhone, setDeletePhone] = useState('');
  const [phoneDialogOpen, setPhoneDialogOpen] = useState(false);
  const [confirmDialogOpen, setConfirmDialogOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [deleteResults, setDeleteResults] = useState<DeleteResult[]>([]);
  
  // Mock data for demonstration - in real app this would come from upload page or search
  const [users] = useState<MatchedUser[]>([]);

  const handleDeleteSelected = async () => {
    if (selectedUsers.length === 0) return;

    setLoading(true);
    setError(null);

    try {
      const response = await ApiService.deleteSelectedUsers(selectedUsers);
      setDeleteResults(response.results);
      
      const deletedCount = response.results.filter(r => r.userDeleted).length;
      setSuccess(`Successfully deleted ${deletedCount} user(s)`);
      setSelectedUsers([]);
      setConfirmDialogOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Delete failed');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteByPhone = async () => {
    if (!deletePhone.trim()) {
      setError('Please enter a phone number');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await ApiService.deleteByPhone(deletePhone.trim());
      setDeleteResults(response.results);
      
      if (response.results.length === 0) {
        setSuccess(response.message || 'No users found with this phone number');
      } else {
        const deletedCount = response.results.filter(r => r.userDeleted).length;
        setSuccess(`Successfully deleted ${deletedCount} user(s)`);
      }
      
      setDeletePhone('');
      setPhoneDialogOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Delete failed');
    } finally {
      setLoading(false);
    }
  };

  const handleCloseSnackbar = () => {
    setSuccess(null);
    setError(null);
  };

  const renderDeleteResults = () => {
    if (deleteResults.length === 0) return null;

    return (
      <Paper sx={{ p: 3, mt: 4 }}>
        <Typography variant="h6" gutterBottom>
          Delete Results
        </Typography>
        
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          {deleteResults.map((result, index) => (
            <Box key={index} sx={{ p: 2, border: 1, borderColor: 'grey.300', borderRadius: 1 }}>
              <Typography variant="subtitle2" gutterBottom>
                User ID: {result.userId}
              </Typography>
              
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                <Chip
                  label={result.userDeleted ? 'User Deleted' : 'User Not Deleted'}
                  color={result.userDeleted ? 'success' : 'error'}
                  size="small"
                />
                
                {result.businessId && (
                  <Chip
                    label={result.businessDeleted ? 'Business Deleted' : 'Business Kept'}
                    color={result.businessDeleted ? 'warning' : 'info'}
                    size="small"
                  />
                )}
                
                {result.error && (
                  <Chip
                    label={`Error: ${result.error}`}
                    color="error"
                    size="small"
                  />
                )}
              </Box>
            </Box>
          ))}
        </Box>
      </Paper>
    );
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Manage Users
      </Typography>
      
      <Typography variant="body1" color="text.secondary" sx={{ mb: 4 }}>
        Delete users individually or in bulk. Users and their associated businesses will be removed from the system.
      </Typography>

      {/* Action Buttons */}
      <Paper sx={{ p: 3, mb: 4 }}>
        <Typography variant="h6" gutterBottom>
          Delete Operations
        </Typography>
        
        <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
          <Button
            variant="contained"
            color="error"
            startIcon={<DeleteIcon />}
            onClick={() => setConfirmDialogOpen(true)}
            disabled={selectedUsers.length === 0 || loading}
          >
            Delete Selected ({selectedUsers.length})
          </Button>
          
          <Button
            variant="outlined"
            color="error"
            startIcon={<PhoneIcon />}
            onClick={() => setPhoneDialogOpen(true)}
            disabled={loading}
          >
            Delete by Phone
          </Button>
        </Box>
      </Paper>

      {/* User Table */}
      {users.length > 0 ? (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Users
          </Typography>
          <UserTable
            users={users}
            selectedUsers={selectedUsers}
            onSelectionChange={setSelectedUsers}
            showSelection
          />
        </Paper>
      ) : (
        <Alert severity="info">
          No users loaded. Upload a phone file or search for users to manage them here.
        </Alert>
      )}

      {/* Delete Results */}
      {renderDeleteResults()}

      {/* Confirmation Dialog for Selected Users */}
      <Dialog open={confirmDialogOpen} onClose={() => setConfirmDialogOpen(false)}>
        <DialogTitle>Confirm Deletion</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to delete {selectedUsers.length} selected user(s)?
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
            This action cannot be undone. Associated businesses will also be deleted if no other users reference them.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmDialogOpen(false)} disabled={loading}>
            Cancel
          </Button>
          <Button
            onClick={handleDeleteSelected}
            color="error"
            variant="contained"
            disabled={loading}
            startIcon={<DeleteIcon />}
          >
            {loading ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Phone Number Dialog */}
      <Dialog open={phoneDialogOpen} onClose={() => setPhoneDialogOpen(false)}>
        <DialogTitle>Delete by Phone Number</DialogTitle>
        <DialogContent>
          <Typography gutterBottom>
            Enter a phone number to delete all users with that number:
          </Typography>
          <TextField
            fullWidth
            label="Phone Number"
            value={deletePhone}
            onChange={(e) => setDeletePhone(e.target.value)}
            placeholder="Enter phone number"
            sx={{ mt: 2 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPhoneDialogOpen(false)} disabled={loading}>
            Cancel
          </Button>
          <Button
            onClick={handleDeleteByPhone}
            color="error"
            variant="contained"
            disabled={loading || !deletePhone.trim()}
            startIcon={<PhoneIcon />}
          >
            {loading ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Snackbars */}
      <Snackbar
        open={!!success}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
      >
        <Alert onClose={handleCloseSnackbar} severity="success">
          {success}
        </Alert>
      </Snackbar>

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