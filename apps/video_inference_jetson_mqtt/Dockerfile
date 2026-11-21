# Builds the MQTT scheduler. clientif is the same binary as video_inference_jetson.

FROM golang:1.27-alpine AS builder

WORKDIR /app

RUN apk --no-cache add protobuf protobuf-dev

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

ENV PATH="$PATH:$(go env GOPATH)/bin"

COPY go.mod go.sum ./
RUN go mod download

COPY proto/ ./proto/
RUN protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    ./proto/fw.proto ./proto/engine.proto

COPY internal/ ./internal/
COPY apps/video_inference_jetson_mqtt/ ./apps/video_inference_jetson_mqtt/

ARG SERVICE_NAME

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./apps/video_inference_jetson_mqtt/${SERVICE_NAME}

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /bin/service .

CMD ["./service"]
