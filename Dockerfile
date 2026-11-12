FROM golang:1.23-alpine AS builder

ARG SERVICE
ARG GO_BUILD_PARALLELISM=1
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -p=${GO_BUILD_PARALLELISM} -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM alpine:3.20

RUN addgroup -S cartnova && adduser -S cartnova -G cartnova
WORKDIR /app

COPY --from=builder /out/service /app/service
COPY migrations /app/migrations

USER cartnova
EXPOSE 8080

ENTRYPOINT ["/app/service"]
