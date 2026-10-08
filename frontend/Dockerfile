# Minimal Alpine base image pinned to Node 22.23 (LTS)
FROM node:22.23-alpine

WORKDIR /app

# Copy dependency manifests
COPY package.json package-lock.json* ./

# Install project dependencies
RUN npm install

# Expose Vite dev server port
EXPOSE 5173

# Start Vite dev server in host mode for container networking
CMD ["npm", "run", "dev"]
