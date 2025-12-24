# Use Node.js Alpine image for smaller size
FROM node:18-alpine
 
# Set working directory
WORKDIR /usr/src/app
 
# Copy package.json and package-lock.json
COPY package*.json ./
 
# Install dependencies
RUN npm install
 
# Copy the rest of the application
COPY . .
 
# Build the NestJS project
RUN npm run build
 
# Expose backend port
EXPOSE 4000
 
# Start NestJS in production mode
CMD ["npm", "run", "start:prod"]