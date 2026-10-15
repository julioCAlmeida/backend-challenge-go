# syntax=docker/dockerfile:1
# Go version must match go.mod.
FROM golang:1.27.2-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -ldflags="-s -w" -o /out/wallet-service ./cmd/wallet-service \
 && go build -ldflags="-s -w" -o /out/migrate ./cmd/migrate \
 && go build -ldflags="-s -w" -o /out/sqs-provision ./cmd/sqs-provision

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget && adduser -D -H -u 10001 app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080
ENTRYPOINT ["/app/wallet-service"]
