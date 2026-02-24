##################################
# Stage 0: Build frontend module
##################################

FROM node:20-alpine AS frontend-builder

RUN npm install -g pnpm@9

WORKDIR /frontend
COPY frontend/package.json frontend/pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile || pnpm install
COPY frontend/ .
RUN pnpm build

##################################
# Stage 1: Build Go executable
##################################

FROM golang:1.23-alpine AS builder

ARG APP_VERSION=1.0.0

# Enable toolchain auto-download for newer Go versions
ENV GOTOOLCHAIN=auto

# Install build dependencies
RUN apk add --no-cache git make curl

# Install buf for proto descriptor generation
RUN curl -sSL "https://github.com/bufbuild/buf/releases/latest/download/buf-$(uname -s)-$(uname -m)" -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

# Set working directory
WORKDIR /src

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY . .

# Regenerate proto descriptor (ensures embedded descriptor.bin is always up to date)
RUN buf build -o cmd/server/assets/descriptor.bin

# Build the server
RUN CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    go build -ldflags "-X main.version=${APP_VERSION} -s -w" \
    -o /src/bin/deployer-server \
    ./cmd/server

##################################
# Stage 2: Create runtime image
##################################

FROM alpine:3.20

ARG APP_VERSION=1.0.0

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Set timezone
ENV TZ=UTC

# Set working directory
WORKDIR /app

# Copy executable from builder
COPY --from=builder /src/bin/deployer-server /app/bin/deployer-server

# Copy configuration files
COPY --from=builder /src/configs/ /app/configs/

# Copy frontend assets from frontend builder
COPY --from=frontend-builder /frontend/dist /app/frontend-dist

# Create non-root user
RUN addgroup -g 1000 deployer && \
    adduser -D -u 1000 -G deployer deployer && \
    chown -R deployer:deployer /app

# Switch to non-root user
USER deployer:deployer

# Expose gRPC and HTTP ports
EXPOSE 9200 9201

# Set default command
CMD ["/app/bin/deployer-server", "-c", "/app/configs"]

# Labels
LABEL org.opencontainers.image.title="Deployer Service" \
      org.opencontainers.image.description="Deployment Management Service" \
      org.opencontainers.image.version="${APP_VERSION}"
