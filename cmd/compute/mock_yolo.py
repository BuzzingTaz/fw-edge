import time
import grpc
import argparse
import sys
import os

# Add the edge-compute-yolo directory to Python's system path to resolve imports
sys.path.append(os.path.join(os.path.dirname(__file__), "edge-compute-yolo"))
import inference_pb2
import inference_pb2_grpc

def run(tracker_addr="127.0.0.1:5005"):
    print(f"Connecting to Go compute gRPC server at {tracker_addr}...")
    while True:
        try:
            with grpc.insecure_channel(tracker_addr) as channel:
                stub = inference_pb2_grpc.InferenceTrackerStub(channel)
                
                def generate_mock_stream():
                    print("Started streaming mock YOLO bounding boxes...")
                    while True:
                        timestamp_ms = int(time.time() * 1000)
                        # Yield a mock person detection bounding box
                        yield inference_pb2.InferenceResult(
                            timestamp=timestamp_ms,
                            boxes=[
                                inference_pb2.BoundingBox(
                                    class_label="person",
                                    confidence=0.92,
                                    x=100,
                                    y=150,
                                    w=40,
                                    h=80
                                )
                            ]
                        )
                        time.sleep(0.033) # 30 FPS
                        
                stub.StreamResults(generate_mock_stream())
        except grpc.RpcError as e:
            print(f"Connection lost: {e}. Reconnecting in 2 seconds...")
            time.sleep(2)
        except KeyboardInterrupt:
            print("Shutting down mock YOLO client.")
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracker-port", type=int, default=5005)
    args = parser.parse_args()
    run(tracker_addr=f"127.0.0.1:{args.tracker_port}")
