import time
import grpc
import threading
import traceback
from concurrent import futures

import av
from ultralytics import YOLO

import proto.fw_pb2 as fw_pb2
import proto.fw_pb2_grpc as fw_pb2_grpc

GRPC_PORT = "[::]:9997"
MODEL_PATH = "yolo26n.engine"

def get_codec_name(mime_type: str) -> str:
    mime_lower = mime_type.lower()
    if "vp8" in mime_lower:
        return "vp8"
    if "h264" in mime_lower:
        return "h264"
    if "vp9" in mime_lower:
        return "vp9"
    if "h265" in mime_lower or "hevc" in mime_lower:
        return "hevc"
    return "unknown"

class VideoDecoder:
    def __init__(self):
        self.codec_name = None
        self.codec = None

    def configure(self, codec_name: str):
        if codec_name == "unknown":
            raise ValueError(f"Unsupported codec specified.")

        if codec_name == self.codec_name and self.codec is not None:
            return

        self.codec_name = codec_name
        self.codec = av.codec.CodecContext.create(codec_name, "r")
        print(f"Configured video decoder for {codec_name}")

    def decode(self, encoded_data: bytes):
        if self.codec is None or not encoded_data:
            return None

        try:
            for packet in self.codec.parse(encoded_data):
                for frame in self.codec.decode(packet):
                    return frame.to_ndarray(format="bgr24")
        except av.error.InvalidDataError:
            return None

        return None

class ComputeStreamServicer(fw_pb2_grpc.ComputeStreamServicer):
    def __init__(self, model):
        self.model = model
        self.inference_lock = threading.Lock()

    def StreamEncodedFrames(self, request_iterator, context):
        print("New client connected to StreamEncodedFrames via gRPC")
        decoder = VideoDecoder()
        decode_errors = 0

        try:
            for encoded_frame in request_iterator:
                task_id = encoded_frame.task_id
                mime_type = encoded_frame.mime_type

                try:
                    sample = fw_pb2.MediaSample.FromString(encoded_frame.data)
                except Exception as e:
                    print(f"Failed to unmarshal MediaSample protobuf for task {task_id}: {e}")
                    continue

                codec_name = get_codec_name(mime_type)
                try:
                    decoder.configure(codec_name)
                except ValueError as err:
                    print(err)
                    continue

                frame = decoder.decode(sample.data)
                if frame is None:
                    decode_errors += 1
                    if decode_errors <= 5 or decode_errors % 100 == 0:
                        print(f"Decode skipped ({decoder.codec_name}, task_id={task_id})")
                    continue

                with self.inference_lock:
                    results = self.model(frame, stream=True, conf=0.5, verbose=False)

                grpc_boxes = []
                for r in results:
                    if len(r.boxes) > 0:
                        xywh = r.boxes.xywh.cpu().numpy()
                        confs = r.boxes.conf.cpu().numpy()
                        cls_ids = r.boxes.cls.cpu().numpy()

                        for i in range(len(xywh)):
                            c_id = int(cls_ids[i].item())

                            if hasattr(self.model, "names") and isinstance(self.model.names, dict):
                                c_name = str(self.model.names.get(c_id, f"Class_{c_id}"))
                            else:
                                c_name = str(c_id)

                            grpc_boxes.append(
                                fw_pb2.BoundingBox(
                                    x=int(xywh[i][0].item()),
                                    y=int(xywh[i][1].item()),
                                    dx=int(xywh[i][2].item()),
                                    dy=int(xywh[i][3].item()),
                                    label=c_name,
                                    confidence=float(confs[i].item()),
                                )
                            )

                yield fw_pb2.InferenceResult(
                    task_id=task_id,
                    processing_status=0,
                    detections=grpc_boxes
                )

        except grpc.RpcError as e:
            print(f"gRPC stream abruptly disconnected: {e.code()}")
        except Exception:
            print("\n" + "=" * 50)
            print("CRITICAL ERROR INSIDE GRPC GENERATOR:")
            traceback.print_exc()
            print("=" * 50 + "\n")
        finally:
            print("gRPC client stream closed.")

def serve():
    print(f"Loading TensorRT model: {MODEL_PATH}...")
    trt_model = YOLO(MODEL_PATH, task="detect")

    # Configure server to handle up to 10 concurrent streams
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', 16 * 1024 * 1024),
            ('grpc.max_receive_message_length', 16 * 1024 * 1024),
        ]
    )

    fw_pb2_grpc.add_ComputeStreamServicer_to_server(
        ComputeStreamServicer(trt_model), server
    )

    server.add_insecure_port(GRPC_PORT)
    print(f"Python Compute node natively listening on gRPC {GRPC_PORT}")
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nShutting down gRPC server cleanly.")
        server.stop(0)

if __name__ == "__main__":
    serve()
