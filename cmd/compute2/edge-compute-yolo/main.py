import cv2
import time
import grpc
import numpy as np
from ultralytics import YOLO
import traceback
import socket
import struct

import inference_pb2
import inference_pb2_grpc

import argparse

TCP_PORT = 5000
GRPC_SERVER_ADDR = '127.0.0.1:5005'
MODEL_PATH = "yolo26n.engine"


def read_frame_from_tcp(sock):
    """
    Read a JPEG frame from TCP socket.
    Protocol: 4-byte big-endian size header, followed by JPEG data.
    """
    # Read 4-byte size header
    size_data = b''
    while len(size_data) < 4:
        chunk = sock.recv(4 - len(size_data))
        if not chunk:
            return None
        size_data += chunk

    frame_size = struct.unpack('>I', size_data)[0]
    if frame_size > 10_000_000:  # Sanity check: max 10MB
        print(f"Frame size {frame_size} too large, skipping")
        return None

    # Read JPEG data
    frame_data = b''
    while len(frame_data) < frame_size:
        chunk = sock.recv(min(65536, frame_size - len(frame_data)))
        if not chunk:
            return None
        frame_data += chunk

    # Decode JPEG to numpy array
    nparr = np.frombuffer(frame_data, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    return frame


def generate_inference_stream(sock, model):
    """
    Generator that reads JPEG frames from TCP socket, runs YOLO inference,
    and yields InferenceResult messages.
    """
    frame_count = 0
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 300

    try:
        while True:
            frame = read_frame_from_tcp(sock)
            print(f"READ frame: {'OK' if frame is not None else 'FAIL'}, shape: {frame.shape if frame is not None else 'N/A'}")

            if frame is None:
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"Too many consecutive read failures, breaking")
                    break
                time.sleep(0.01)
                continue

            consecutive_failures = 0
            frame_count += 1
            if frame_count == 10:
                cv2.imwrite("/app/debug_frame.jpg", frame)
                print("Saved debug_frame.jpg!")

            # Run inference
            results = model(frame, stream=True, conf=0.5, verbose=False)
            timestamp_ms = int(time.time() * 1000)

            for r in results:
                grpc_boxes = []
                if len(r.boxes) > 0:
                    xywh = r.boxes.xywh.cpu().numpy()
                    confs = r.boxes.conf.cpu().numpy()
                    cls_ids = r.boxes.cls.cpu().numpy()

                    for i in range(len(xywh)):
                        c_id = int(cls_ids[i].item())

                        if hasattr(model, 'names') and isinstance(model.names, dict):
                            c_name = str(model.names.get(c_id, f"Class_{c_id}"))
                        else:
                            c_name = str(c_id)

                        grpc_boxes.append(inference_pb2.BoundingBox(
                            class_label=c_name,
                            confidence=float(confs[i].item()),
                            x=int(xywh[i][0].item()),
                            y=int(xywh[i][1].item()),
                            w=int(xywh[i][2].item()),
                            h=int(xywh[i][3].item())
                        ))

                yield inference_pb2.InferenceResult(
                    timestamp=timestamp_ms,
                    boxes=grpc_boxes
                )
    except Exception as e:
        print("\n" + "="*50)
        print("CRITICAL PYTHON ERROR INSIDE GENERATOR:")
        traceback.print_exc()
        print("="*50 + "\n")
        raise e


def run(model_path="yolo26n.engine", tcp_port=5000, tracker_addr="127.0.0.1:5005"):
    print(f"Loading TensorRT model: {model_path}...")
    trt_model = YOLO(model_path, task='detect')

    # Create TCP server socket to receive JPEG frames from compute
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('127.0.0.1', tcp_port))
    server_sock.listen(1)
    print(f"TCP server listening on port {tcp_port} for JPEG frames from compute")

    while True:
        print(f"Waiting for compute TCP connection...")
        conn, addr = server_sock.accept()
        print(f"Compute connected from {addr}")

        print(f"Connecting to Go gRPC server at {tracker_addr}...")
        try:
            with grpc.insecure_channel(tracker_addr) as channel:
                stub = inference_pb2_grpc.InferenceTrackerStub(channel)

                print("Streaming inferences to Go server...")
                response = stub.StreamResults(
                    generate_inference_stream(conn, trt_model))

                print("StreamResults returned")

        except grpc.RpcError as e:
            print(f"gRPC connection lost: {e}. Reconnecting in 2 seconds")
            time.sleep(2)
        except KeyboardInterrupt:
            print("Shutting down cleanly.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            traceback.print_exc()
            time.sleep(2)
        finally:
            conn.close()
            print("Compute connection closed")

    server_sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default="yolo26n.engine")
    parser.add_argument("--tcp-port", type=int, default=5000)
    parser.add_argument("--tracker-port", type=int, default=5005)
    args = parser.parse_args()

    run(model_path=args.model, tcp_port=args.tcp_port, tracker_addr=f"127.0.0.1:{args.tracker_port}")
