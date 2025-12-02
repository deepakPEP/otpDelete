import React, { useState, useCallback } from 'react';
import {
  Box,
  Button,
  Typography,
  Paper,
  Alert,
  LinearProgress,
} from '@mui/material';
import { CloudUpload } from '@mui/icons-material';

interface FileUploadProps {
  onFileUpload: (file: File) => Promise<void>;
  loading?: boolean;
  accept?: string;
  maxSize?: number; // in MB
}

export default function FileUpload({
  onFileUpload,
  loading = false,
  accept = '.csv,.json',
  maxSize = 10,
}: FileUploadProps) {
  const [dragOver, setDragOver] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const validateFile = (file: File): string | null => {
    // Check file size
    if (file.size > maxSize * 1024 * 1024) {
      return `File size must be less than ${maxSize}MB`;
    }

    // Check file type
    const allowedTypes = ['.csv', '.json'];
    const fileExtension = '.' + file.name.split('.').pop()?.toLowerCase();
    if (!allowedTypes.includes(fileExtension)) {
      return 'Only CSV and JSON files are allowed';
    }

    return null;
  };

  const handleFile = useCallback(async (file: File) => {
    setError(null);
    const validationError = validateFile(file);
    if (validationError) {
      setError(validationError);
      return;
    }
    try {
      await onFileUpload(file);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed');
    }
  }, [onFileUpload, maxSize, setError, validateFile]);

  const handleDrop = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(false);
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      handleFile(files[0]);
    }
  }, [handleFile]);

  const handleDragOver = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(false);
  }, []);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      handleFile(files[0]);
    }
  };

  return (
    <Box>
      <Paper
        elevation={dragOver ? 4 : 1}
        sx={{
          p: 4,
          border: 2,
          borderColor: dragOver ? 'primary.main' : 'grey.300',
          borderStyle: 'dashed',
          textAlign: 'center',
          cursor: loading ? 'not-allowed' : 'pointer',
          backgroundColor: dragOver ? 'action.hover' : 'background.paper',
          transition: 'all 0.2s ease',
        }}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
      >
        <input
          type="file"
          accept={accept}
          onChange={handleFileSelect}
          style={{ display: 'none' }}
          id="file-upload-input"
          disabled={loading}
        />
        
        <label htmlFor="file-upload-input" style={{ cursor: loading ? 'not-allowed' : 'pointer' }}>
          <CloudUpload sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            Upload Phone File
          </Typography>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            Drag and drop a CSV or JSON file here, or click to select
          </Typography>
          <Typography variant="caption" color="text.secondary">
            Supported formats: CSV, JSON • Max size: {maxSize}MB
          </Typography>
          
          <Box sx={{ mt: 2 }}>
            <Button
              variant="contained"
              component="span"
              disabled={loading}
              startIcon={<CloudUpload />}
            >
              Choose File
            </Button>
          </Box>
        </label>
      </Paper>

      {loading && (
        <Box sx={{ mt: 2 }}>
          <LinearProgress />
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Processing file...
          </Typography>
        </Box>
      )}

      {error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          {error}
        </Alert>
      )}
    </Box>
  );
}