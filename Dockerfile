FROM golang:1.23-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# No GOARCH here: the image builds natively for whatever machine runs it (amd64 or arm64).
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/workqueue-producer ./cmd/producer \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/workqueue-worker ./cmd/worker

FROM alpine:3.20
RUN adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /out/workqueue-producer /out/workqueue-worker /app/
USER app
# docker-compose.yml picks which binary each service runs.
CMD ["/app/workqueue-producer"]
