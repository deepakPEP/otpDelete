import React from 'react';
import {
  AppBar,
  Toolbar,
  Typography,
  Button,
  Box,
  Container,
} from '@mui/material';
import { Home, CloudUpload, Search, Delete, VpnKey } from '@mui/icons-material';
import Link from 'next/link';
import { useRouter } from 'next/router';

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const router = useRouter();

  const navItems = [
    { label: 'Home', href: '/', icon: <Home /> },
    { label: 'Upload', href: '/upload', icon: <CloudUpload /> },
    { label: 'Search', href: '/search', icon: <Search /> },
    { label: 'OTP', href: '/otp', icon: <VpnKey /> },
    { label: 'Manage', href: '/manage', icon: <Delete /> },
  ];

  const isActive = (href: string) => router.pathname === href;

  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            User Management System
          </Typography>
          
          <Box sx={{ display: 'flex', gap: 1 }}>
            {navItems.map((item) => (
              <Link key={item.href} href={item.href} passHref legacyBehavior>
                <Button
                  color="inherit"
                  startIcon={item.icon}
                  sx={{
                    backgroundColor: isActive(item.href) ? 'rgba(255,255,255,0.1)' : 'transparent',
                  }}
                >
                  {item.label}
                </Button>
              </Link>
            ))}
          </Box>
        </Toolbar>
      </AppBar>

      <Container maxWidth="lg" sx={{ py: 4 }}>
        {children}
      </Container>
    </Box>
  );
}