import React, { useState } from 'react';
import {
  Box,
  Typography,
  Alert,
  Snackbar,
  Paper,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import { Delete as DeleteIcon, DeleteSweep, Warning } from '@mui/icons-material';
import FileUpload from '@/components/FileUpload';
import UserTable from '@/components/UserTable';
import ApiService from '@/services/api';
import type { MatchedUser } from '@/types';

export default function Upload() {
  const [matchedUsers, setMatchedUsers] = useState<MatchedUser[]>([]);
  const [selectedUsers, setSelectedUsers] = useState<string[]>([]);
  const [originalPhones, setOriginalPhones] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [success, setSuccess] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmDialog, setConfirmDialog] = useState<{
    open: boolean;
    type: 'selected' | 'all';
    count: number;
  }>({ open: false, type: 'selected', count: 0 });

  const handleFileUpload = async (file: File) => {
    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      // Extract phone numbers from file for later use
      const text = await file.text();
      let phones: string[] = [];
      
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
          if (parsed.length > 0 && typeof parsed[0] === 'string') {
            phones = parsed;
          } else {
            phones = parsed.map((p: unknown) => {
              if (typeof p === 'object' && p !== null) {
                const obj = p as Record<string, any>;
                return obj.phoneNo || obj.phone || obj.Phone || obj.mobile || '';
              }
              return '';
            }).filter(Boolean);
          }
        }
      } catch {
        // Parse as CSV
        const lines = text.split('\n').filter(line => line.trim());
        if (lines.length > 1) {
          phones = lines.slice(1).map(line => line.trim()).filter(Boolean);
        }
      }
      
      setOriginalPhones(phones);
      
      const response = await ApiService.uploadPhoneFile(file);
      setMatchedUsers(response.matched);
      setSelectedUsers([]);
      setSuccess(`Successfully processed file. Found ${response.matched.length} matching users.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed');
      setMatchedUsers([]);
      setOriginalPhones([]);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteSelected = async () => {
    if (selectedUsers.length === 0) return;
    
    setDeleteLoading(true);
    setError(null);

    try {
      const response = await ApiService.deleteSelectedUsers(selectedUsers);
      const deletedCount = response.results.filter(r => r.userDeleted).length;
      
      // Remove deleted users from the list
      setMatchedUsers(prev => prev.filter(user => !selectedUsers.includes(user.userId)));
      setSelectedUsers([]);
      setSuccess(`Successfully deleted ${deletedCount} user(s)`);
      setConfirmDialog({ open: false, type: 'selected', count: 0 });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Delete failed');
    } finally {
      setDeleteLoading(false);
    }
  };

  const handleDeleteAll = async () => {
    if (originalPhones.length === 0) return;
    
    setDeleteLoading(true);
    setError(null);

    try {
      const response = await ApiService.deleteAllFromFile(originalPhones);
      const deletedCount = response.results.filter(r => r.userDeleted).length;
      
      // Clear all matched users
      setMatchedUsers([]);
      setSelectedUsers([]);
      setOriginalPhones([]);
      setSuccess(`Successfully deleted ${deletedCount} user(s) from the uploaded file`);
      setConfirmDialog({ open: false, type: 'all', count: 0 });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Delete failed');
    } finally {
      setDeleteLoading(false);
    }
  };

  const openConfirmDialog = (type: 'selected' | 'all') => {
    const count = type === 'selected' ? selectedUsers.length : matchedUsers.length;
    setConfirmDialog({ open: true, type, count });
  };

  const handleCloseSnackbar = () => {
    setSuccess(null);
    setError(null);
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Upload Phone File
      </Typography>
      
      <Typography variant="body1" color="text.secondary" sx={{ mb: 4 }}>
        Upload a CSV or JSON file containing phone numbers to find matching users in the system.
      </Typography>

      <FileUpload
        onFileUpload={handleFileUpload}
        loading={loading}
      />

      {matchedUsers.length > 0 && (
        <Paper sx={{ mt: 4, p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h6">
              Matched Users ({matchedUsers.length})
            </Typography>
            
            <Box sx={{ display: 'flex', gap: 2 }}>
              <Button
                variant="outlined"
                color="error"
                startIcon={<DeleteIcon />}
                onClick={() => openConfirmDialog('selected')}
                disabled={selectedUsers.length === 0 || deleteLoading}
              >
                Delete Selected ({selectedUsers.length})
              </Button>
              
              <Button
                variant="contained"
                color="error"
                startIcon={<DeleteSweep />}
                onClick={() => openConfirmDialog('all')}
                disabled={deleteLoading}
              >
                Delete All from File
              </Button>
            </Box>
          </Box>
          
          <UserTable 
            users={matchedUsers} 
            selectedUsers={selectedUsers}
            onSelectionChange={setSelectedUsers}
            showSelection={true}
          />
        </Paper>
      )}

      {/* Confirmation Dialog */}
      <Dialog 
        open={confirmDialog.open} 
        onClose={() => setConfirmDialog({ open: false, type: 'selected', count: 0 })}
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Warning color="warning" />
          Confirm Deletion
        </DialogTitle>
        <DialogContent>
          <Typography gutterBottom>
            {confirmDialog.type === 'selected' 
              ? `Are you sure you want to delete ${confirmDialog.count} selected user(s)?`
              : `Are you sure you want to delete ALL ${matchedUsers.length} user(s) from this uploaded file?`
            }
          </Typography>
          <Typography variant="body2" color="text.secondary">
            This action cannot be undone. Associated businesses will also be deleted if no other users reference them.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button 
            onClick={() => setConfirmDialog({ open: false, type: 'selected', count: 0 })}
            disabled={deleteLoading}
          >
            Cancel
          </Button>
          <Button
            onClick={confirmDialog.type === 'selected' ? handleDeleteSelected : handleDeleteAll}
            color="error"
            variant="contained"
            disabled={deleteLoading}
            startIcon={confirmDialog.type === 'selected' ? <DeleteIcon /> : <DeleteSweep />}
          >
            {deleteLoading ? 'Deleting...' : `Delete ${confirmDialog.type === 'selected' ? 'Selected' : 'All'}`}
          </Button>
        </DialogActions>
      </Dialog>

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