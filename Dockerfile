FROM golang:1.27-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -o /processor ./cmd/processor

FROM alpine:3.20

WORKDIR /app

COPY --from=build /processor ./processor
COPY schemas/ ./schemas/

CMD ["./processor"]
