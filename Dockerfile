# Build stage
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /raftnova ./cmd/server

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 1000 raftnova

COPY --from=builder /raftnova /usr/local/bin/raftnova

USER raftnova
WORKDIR /home/raftnova

EXPOSE 8080 9090

ENTRYPOINT ["raftnova"]
