FROM golang:1.27-alpine AS builder

WORKDIR /app

# Install system dependencies
RUN apk --no-cache add build-base protobuf protobuf-dev

# Install the protobuf compiler, go protobuf and grpc plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
ENV PATH="$PATH:$(go env GOPATH)/bin"

COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Generate the Go and gRPC code.
RUN protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    ./proto/fw.proto ./proto/engine.proto

# Build the scheduler binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/scheduler ./apps/video_inference_jetson/scheduler

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /bin/scheduler ./scheduler

# When the scheduler container starts, it should announce itself to Central NATS
# We will just run the binary
CMD ["./scheduler"]
