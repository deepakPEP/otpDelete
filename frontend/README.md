# User Management System Frontend

A Next.js TypeScript application for managing users and businesses through phone number matching.

## Features

- **File Upload**: Upload CSV or JSON files containing phone numbers to find matching users
- **User Search**: Search for users by phone number and view their details
- **User Management**: Delete users individually or in bulk operations
- **Business Integration**: View and manage associated business information

## Getting Started

### Prerequisites

- Node.js 18+ 
- Backend API running on `http://localhost:4000` (configure in `.env.local`)

### Installation

```bash
npm install
```

### Environment Setup

Create a `.env.local` file:

```bash
NEXT_PUBLIC_API_URL=http://localhost:4000
```

### Development

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to view the application.

## Project Structure

```
src/
├── components/          # Reusable UI components
│   ├── FileUpload.tsx   # File upload with drag & drop
│   ├── Layout.tsx       # App layout with navigation
│   └── UserTable.tsx    # User data table with selection
├── pages/               # Next.js pages
│   ├── index.tsx        # Home page
│   ├── upload.tsx       # File upload page
│   ├── search.tsx       # User search page
│   ├── manage.tsx       # User management page
│   └── _app.tsx         # App wrapper with theme
├── services/            # API integration
│   └── api.ts           # Backend API calls
└── types/               # TypeScript type definitions
    └── index.ts         # Shared types
```

## API Integration

The frontend integrates with the following backend endpoints:

- `POST /api/upload-phonefile` - Upload phone file
- `GET /api/matches?phone=` - Search users by phone
- `POST /api/delete-selected` - Delete users by IDs
- `POST /api/delete-by-phone` - Delete users by phone number

## Technologies Used

- Next.js 14 with TypeScript
- Material-UI for components and styling
- Axios for API calls
- React hooks for state management

## File Formats Supported

### CSV Format
```csv
phoneNo
+1234567890
+1987654321
```

### JSON Format
```json
["+1234567890", "+1987654321"]
```

Or with objects:
```json
[
  {"phoneNo": "+1234567890"},
  {"phone": "+1987654321"}
]
```

## Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run start` - Start production server
- `npm run lint` - Run ESLint
