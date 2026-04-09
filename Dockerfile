FROM golang:1.21-alpine AS builder

ARG GIT_COMMIT=unknown
ARG VERSION=dev
ARG BUILD_TIME=unknown

RUN apk add --no-cache ca-certificates git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w \
      -X github.com/omersiar/ript/internal/version.Version=${VERSION} \
      -X github.com/omersiar/ript/internal/version.GitCommit=${GIT_COMMIT} \
      -X github.com/omersiar/ript/internal/version.BuildTime=${BUILD_TIME}" \
    -o /app/ript ./cmd/ript

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/ript /ript
COPY --from=builder /app/web /web

USER 65534:65534

EXPOSE 8080

ENV RIPT_KAFKA_BROKERS=kafka:9092
ENV RIPT_SCAN_INTERVAL_MINUTES=60
ENV RIPT_HTTP_HOST=0.0.0.0
ENV RIPT_HTTP_PORT=8080

ENTRYPOINT ["/ript"]
