# Multi-stage build for all Go services
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git gcc musl-dev librdkafka-dev pkgconf

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build all services
RUN CGO_ENABLED=1 go build -o /bin/gateway ./cmd/gateway
RUN CGO_ENABLED=1 go build -o /bin/worker ./cmd/worker
RUN CGO_ENABLED=1 go build -o /bin/archive-writer ./cmd/archive-writer
RUN CGO_ENABLED=1 go build -o /bin/detection ./cmd/detection
RUN CGO_ENABLED=1 go build -o /bin/query-coordinator ./cmd/query-coordinator
RUN CGO_ENABLED=1 go build -o /bin/alert-service ./cmd/alert-service
RUN CGO_ENABLED=0 go build -o /bin/load-generator ./cmd/load-generator

# Runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates librdkafka

COPY --from=builder /bin/gateway /bin/gateway
COPY --from=builder /bin/worker /bin/worker
COPY --from=builder /bin/archive-writer /bin/archive-writer
COPY --from=builder /bin/detection /bin/detection
COPY --from=builder /bin/query-coordinator /bin/query-coordinator
COPY --from=builder /bin/alert-service /bin/alert-service
COPY --from=builder /bin/load-generator /bin/load-generator
