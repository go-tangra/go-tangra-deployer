# syntax=docker/dockerfile:1
# Deployer service image: builds the Vue remote, embeds it (-tags ui), and
# produces a slim runtime carrying deployersvc. Build context is the repo root
# so the module's replace directives (../.. and sibling services) resolve.

FROM node:22-alpine AS ui
WORKDIR /ui
COPY services/deployer/ui/package.json services/deployer/ui/package-lock.json* ./
RUN npm ci --no-audit --no-fund || npm install --no-audit --no-fund
COPY services/deployer/ui/ ./
RUN npm run build

FROM golang:1.26-alpine AS build
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY . .
COPY --from=ui /ui/dist ./services/deployer/ui/dist
WORKDIR /src/services/deployer
ENV CGO_ENABLED=0 GOFLAGS=-buildvcs=false
RUN go build -tags "ui" -o /out/deployersvc ./cmd/deployersvc

FROM alpine:3.20
RUN apk add --no-cache ca-certificates postgresql-client && adduser -D -u 10001 app
COPY --from=build /out/deployersvc /usr/local/bin/
COPY services/deployer/deploy /app/deploy
WORKDIR /app
USER app
ENTRYPOINT ["deployersvc"]
CMD ["-config", "deploy/container.yaml"]
