# Build Stage
FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o inventory-service main.go

# Run Stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/inventory-service .

EXPOSE 8080

CMD ["./inventory-service"]
