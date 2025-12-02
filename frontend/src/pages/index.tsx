import React from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Button,
  Paper,
} from '@mui/material';
import {
  CloudUpload,
  Search,
  Delete,
  People,
  VpnKey,
} from '@mui/icons-material';
import Link from 'next/link';

export default function Home() {
  const features = [
    {
      title: 'Upload Phone Files',
      description: 'Upload CSV or JSON files containing phone numbers to find matching users',
      icon: <CloudUpload sx={{ fontSize: 40 }} />,
      href: '/upload',
      color: 'primary.main',
    },
    {
      title: 'Search Users',
      description: 'Search for users by phone number and view their details',
      icon: <Search sx={{ fontSize: 40 }} />,
      href: '/search',
      color: 'secondary.main',
    },
    {
      title: 'OTP Lookup',
      description: 'Find the latest One-Time Password for any phone number',
      icon: <VpnKey sx={{ fontSize: 40 }} />,
      href: '/otp',
      color: 'success.main',
    },
    {
      title: 'Manage Users',
      description: 'Delete users individually or in bulk, manage user data',
      icon: <Delete sx={{ fontSize: 40 }} />,
      href: '/manage',
      color: 'error.main',
    },
  ];

  return (
    <Box>
      {/* Hero Section */}
      <Paper
        elevation={0}
        sx={{
          bgcolor: 'primary.main',
          color: 'primary.contrastText',
          p: 6,
          borderRadius: 2,
          mb: 4,
          textAlign: 'center',
        }}
      >
        <People sx={{ fontSize: 60, mb: 2 }} />
        <Typography variant="h3" component="h1" gutterBottom>
          User Management System
        </Typography>
        <Typography variant="h6" sx={{ opacity: 0.9 }}>
          Efficiently manage users and businesses through phone number matching
        </Typography>
      </Paper>

      {/* Features Grid */}
      <Box 
        sx={{ 
          display: 'grid', 
          gridTemplateColumns: { xs: '1fr', md: 'repeat(3, 1fr)' },
          gap: 4 
        }}
      >
        {features.map((feature, index) => (
          <Box key={index}>
            <Card
              sx={{
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                transition: 'transform 0.2s ease-in-out',
                '&:hover': {
                  transform: 'translateY(-4px)',
                },
              }}
            >
              <CardContent sx={{ flexGrow: 1, textAlign: 'center', p: 3 }}>
                <Box sx={{ color: feature.color, mb: 2 }}>
                  {feature.icon}
                </Box>
                <Typography variant="h5" component="h2" gutterBottom>
                  {feature.title}
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                  {feature.description}
                </Typography>
                <Link href={feature.href} passHref legacyBehavior>
                  <Button variant="contained" fullWidth>
                    Get Started
                  </Button>
                </Link>
              </CardContent>
            </Card>
          </Box>
        ))}
      </Box>

      {/* Quick Stats or Info */}
      <Paper sx={{ p: 4, mt: 4, textAlign: 'center' }}>
        <Typography variant="h5" gutterBottom>
          How It Works
        </Typography>
        <Box 
          sx={{ 
            display: 'grid', 
            gridTemplateColumns: { xs: '1fr', md: 'repeat(4, 1fr)' },
            gap: 3,
            mt: 2 
          }}
        >
          <Box>
            <Typography variant="h6" color="primary" gutterBottom>
              1. Upload
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Upload a CSV or JSON file containing phone numbers
            </Typography>
          </Box>
          <Box>
            <Typography variant="h6" color="primary" gutterBottom>
              2. Match
            </Typography>
            <Typography variant="body2" color="text.secondary">
              System finds users and businesses matching those phone numbers
            </Typography>
          </Box>
          <Box>
            <Typography variant="h6" color="primary" gutterBottom>
              3. Verify
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Lookup OTPs for phone verification and authentication
            </Typography>
          </Box>
          <Box>
            <Typography variant="h6" color="primary" gutterBottom>
              4. Manage
            </Typography>
            <Typography variant="body2" color="text.secondary">
              View, search, and manage users and their associated businesses
            </Typography>
          </Box>
        </Box>
      </Paper>
    </Box>
  );
}