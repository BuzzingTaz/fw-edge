# Dockerfile used by compose to build an individual service.
# Copies sources to a go container, builds the binary named by SERVICE_NAME,
# and then copies it to a minimal alpine image for execution.

# --- Build Stage ---
FROM golang:1.27-alpine AS builder

WORKDIR /app

# Install the protobuf compiler, go protobuf and grpc plugins
RUN apk --no-cache add protobuf protobuf-dev

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

ENV PATH="$PATH:$(go env GOPATH)/bin"

COPY go.mod go.sum ./
RUN go mod download

# Generate the Go and gRPC code.
COPY proto/ ./proto/
RUN protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    ./proto/fw.proto ./proto/engine.proto

# Copy the shared internal packages and this app
COPY internal/ ./internal/
COPY apps/video_inference_jetson/ ./apps/video_inference_jetson/

# Pass the name of the service we want to build
ARG SERVICE_NAME

# Build the specific binary statically
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./apps/video_inference_jetson/${SERVICE_NAME}

# --- Run Stage ---
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /bin/service .

CMD ["./service"]
