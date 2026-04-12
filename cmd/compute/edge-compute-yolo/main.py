import cv2
import time
import grpc
import numpy as np
from ultralytics import YOLO
import traceback

import inference_pb2
import inference_pb2_grpc

UDP_PORT = 5000
GRPC_SERVER_ADDR = '127.0.0.1:5005'
MODEL_PATH = "yolo26n.engine"


def generate_inference_stream(cap, model):
    """
    This is a Python generator. It yields InferenceResult messages
    as long as the video stream is active. gRPC will pipe these
    yields directly over the wire.
    """
    frame_count = 0
    try:
        while cap.isOpened():
            ret, frame = cap.read()
            print(f"Read frame: {ret}, shape: {frame.shape if ret else 'N/A'}")
            if not ret:
                time.sleep(0.005)
                continue
            frame_count += 1
            if frame_count == 10:
                cv2.imwrite("/app/debug_frame.jpg", frame)
                print("\/app/debug_frame.jpg!\n")
            # Run inference (stream=True prevents memory leaks)
            results = model(frame, stream=True, conf=0.1, verbose=False)

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

                # Yield the message to the active gRPC stream
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


def run():
    print(f"Loading TensorRT model: {MODEL_PATH}...")
    trt_model = YOLO(MODEL_PATH, task='detect')

    gst_pipeline = (
        f"udpsrc port={UDP_PORT} caps=\"application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)VP8\" ! "
        "rtpjitterbuffer latency=50 ! "
        "rtpvp8depay ! nvv4l2decoder ! "
        "nvvidconv ! video/x-raw, format=BGRx ! "
        "videoconvert ! video/x-raw, format=BGR ! appsink drop=1"
    )

    while True:
        print(f"Connecting to Go gRPC server at {GRPC_SERVER_ADDR}...")

        try:
            with grpc.insecure_channel(GRPC_SERVER_ADDR) as channel:
                stub = inference_pb2_grpc.InferenceTrackerStub(channel)

                print(f"Opening GStreamer pipeline on UDP port {UDP_PORT}")
                cap = cv2.VideoCapture(gst_pipeline, cv2.CAP_GSTREAMER)

                if not cap.isOpened():
                    print("Waiting for GStreamer pipeline")
                    time.sleep(2)
                    continue

                print("Streaming inferences to Go server...")
                response = stub.StreamResults(
                    generate_inference_stream(cap, trt_model))  # Blocking

                cap.release()

        except grpc.RpcError as e:
            print(f"gRPC connection lost: {e}. Reconnecting in 2 seconds")
            time.sleep(2)
        except KeyboardInterrupt:
            print("Shutting down cleanly.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}. Restarting in 2 seconds.")
            time.sleep(2)


if __name__ == "__main__":
    run()
