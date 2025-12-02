import React from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Checkbox,
  Chip,
  Typography,
} from '@mui/material';
import type { MatchedUser, SearchMatch } from '@/types';

interface UserTableProps {
  users: MatchedUser[] | SearchMatch[];
  selectedUsers?: string[];
  onSelectionChange?: (userIds: string[]) => void;
  showSelection?: boolean;
}

export default function UserTable({
  users,
  selectedUsers = [],
  onSelectionChange,
  showSelection = false,
}: UserTableProps) {
  const handleSelectAll = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      onSelectionChange?.(users.map(user => user.userId));
    } else {
      onSelectionChange?.([]);
    }
  };

  const handleSelectUser = (userId: string) => {
    const newSelection = selectedUsers.includes(userId)
      ? selectedUsers.filter(id => id !== userId)
      : [...selectedUsers, userId];
    onSelectionChange?.(newSelection);
  };

  const isSelected = (userId: string) => selectedUsers.includes(userId);
  const allSelected = users.length > 0 && selectedUsers.length === users.length;
  const someSelected = selectedUsers.length > 0 && selectedUsers.length < users.length;

  if (users.length === 0) {
    return (
      <Typography variant="body1" color="text.secondary" sx={{ textAlign: 'center', py: 4 }}>
        No users found
      </Typography>
    );
  }

  return (
    <TableContainer component={Paper}>
      <Table>
        <TableHead>
          <TableRow>
            {showSelection && (
              <TableCell padding="checkbox">
                <Checkbox
                  indeterminate={someSelected}
                  checked={allSelected}
                  onChange={handleSelectAll}
                />
              </TableCell>
            )}
            <TableCell>User ID</TableCell>
            <TableCell>Name</TableCell>
            <TableCell>Phone Number</TableCell>
            <TableCell>Business ID</TableCell>
            {'businessExists' in users[0] && <TableCell>Business Status</TableCell>}
          </TableRow>
        </TableHead>
        <TableBody>
          {users.map((user) => (
            <TableRow
              key={user.userId}
              selected={isSelected(user.userId)}
              hover={showSelection}
            >
              {showSelection && (
                <TableCell padding="checkbox">
                  <Checkbox
                    checked={isSelected(user.userId)}
                    onChange={() => handleSelectUser(user.userId)}
                  />
                </TableCell>
              )}
              <TableCell>
                <Typography variant="body2" fontFamily="monospace">
                  {user.userId}
                </Typography>
              </TableCell>
              <TableCell>{user.name || 'N/A'}</TableCell>
              <TableCell>{user.phoneNo}</TableCell>
              <TableCell>
                {user.businessId ? (
                  <Typography variant="body2" fontFamily="monospace">
                    {user.businessId}
                  </Typography>
                ) : (
                  'N/A'
                )}
              </TableCell>
              {'businessExists' in user && (
                <TableCell>
                  <Chip
                    label={user.businessExists ? 'Exists' : 'Missing'}
                    color={user.businessExists ? 'success' : 'warning'}
                    size="small"
                  />
                </TableCell>
              )}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}