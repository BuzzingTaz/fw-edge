import cv2
import time
import grpc
import numpy as np
from ultralytics import YOLO
import traceback

import inference_pb2 #generated from inference.proto, contains the InferenceResult and BoundingBox message definitions
import inference_pb2_grpc #generated from inference.proto, contains the InferenceTrackerStub class which is used to call the gRPC service methods defined in the proto file (like StreamResults)

UDP_PORT = 5000
GRPC_SERVER_ADDR = '127.0.0.1:5005'
MODEL_PATH = "cmd/compute/edge-compute-yolo/yolov8n.engine"



def generate_inference_stream(cap, model):
    """
    This is a Python generator. It yields InferenceResult messages
    as long as the video stream is active. gRPC will pipe these
    yields directly over the wire.
    """
    frame_count = 0
    try:
        while cap.isOpened(): # keep processing until the video stream is closed
            """
            What happens internally:
                GStreamer gets UDP packets
                Decodes them
                Converts to BGR
                Gives frame to OpenCV
            """
            ret, frame = cap.read() #get a single video frame from the GStreamer pipeline. ret is a boolean indicating success, and frame is the actual image data as a numpy array.
            print(f"Read frame: {ret}, shape: {frame.shape if ret else 'N/A'}")
            if not ret:
                time.sleep(0.005)
                continue
            frame_count += 1

            # debugging: save the 10th frame to disk so we can inspect it and make sure the video decoding pipeline is working correctly and that the frames look as expected before they get fed into the YOLO model. This can help us catch any issues with the GStreamer pipeline or the video format early on.
            if frame_count == 10:
                cv2.imwrite("/app/debug_frame.jpg", frame)
                print("\/app/debug_frame.jpg!\n")
            # Run inference (stream=True prevents memory leaks)
            results = model(frame, stream=True, conf=0.1, verbose=False)

            timestamp_ms = int(time.time() * 1000)

            for r in results:
                grpc_boxes = []
                if len(r.boxes) > 0:
                    #Converts GPU tensors → CPU → NumPy
                    xywh = r.boxes.xywh.cpu().numpy()
                    confs = r.boxes.conf.cpu().numpy()
                    cls_ids = r.boxes.cls.cpu().numpy()

                    for i in range(len(xywh)):
                        c_id = int(cls_ids[i].item())
                        
                        # mapping class names
                        if hasattr(model, 'names') and isinstance(model.names, dict):
                            c_name = str(model.names.get(c_id, f"Class_{c_id}"))
                        else:
                            c_name = str(c_id)

                        # converting detection to structured message format defined in inference.proto
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
    trt_model = YOLO(MODEL_PATH, task='detect') #load the model


    """
    note: its a network video decoding pipeline that uses GStreamer to receive video frames over UDP, decode them using NVIDIA's hardware acceleration, and then feed those frames into OpenCV for inference. Here's a breakdown of the pipeline components:

    udpsrc - Listen to UDP port for incoming RTP packets from the Go server
    rtpjitterbuffer - fix network delays, and handles out-of-order packets, ensuring a smooth video stream
    rtpvp8depay - extract VP8 video frames from RTP packets
    nvv4l2decoder - use NVIDIA hardware to decode VP8 frames into raw video frames
    nvvidconv - convert raw video frames to BGRx format(GPU-accelerated) (open cv expects BGR image (Numpy array))
    videoconvert - convert BGRx to BGR format (CPU-based, but necessary for OpenCV compatibility)
    appsink - allow the Python code to read the video frames directly from the GStreamer pipeline
    drop 1 - if the Python code can't keep up with the incoming video frames, drop frames instead of buffering them to prevent latency buildup
    """
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
            with grpc.insecure_channel(GRPC_SERVER_ADDR) as channel: # open connection to the Go server's gRPC endpoint. This will be used to stream inference results back to the Go server.
                stub = inference_pb2_grpc.InferenceTrackerStub(channel) # create a gRPC client stub that allows us to call the StreamResults method defined in the proto file, which the Go server implements. This is how we'll send inference results back to the Go server.

                print(f"Opening GStreamer pipeline on UDP port {UDP_PORT}")
                #connecting GStreamer with openCV
                # cap behaves like a live video feed that we can read frames from in a loop. The frames are coming from the Go server over UDP, being decoded by GStreamer, and fed into OpenCV for inference.
                cap = cv2.VideoCapture(gst_pipeline, cv2.CAP_GSTREAMER) #telling opencv to Use GStreamer pipeline as video source

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
