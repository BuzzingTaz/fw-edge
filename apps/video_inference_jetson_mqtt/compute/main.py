import os
import time
import threading
import socket
import asyncio

import av
from ultralytics import YOLO
import paho.mqtt.client as mqtt
import nats

import proto.fw_pb2 as fw_pb2
import proto.engine_pb2 as engine_pb2
from telemetry_client import telemetry

MQTT_BROKER = os.environ.get("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MODEL_PATH = "yolo26n.engine"
CLEANUP_INTERVAL = 10
TIMEOUT_SECONDS = 60

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
        self.last_active = time.time()

    def configure(self, codec_name: str):
        self.last_active = time.time()
        if codec_name == "unknown":
            raise ValueError("Unsupported codec specified.")

        if codec_name == self.codec_name and self.codec is not None:
            return

        self.codec_name = codec_name
        self.codec = av.codec.CodecContext.create(codec_name, "r")
        print(f"Configured video decoder for {codec_name}")

    def decode(self, encoded_data: bytes):
        self.last_active = time.time()
        if self.codec is None or not encoded_data:
            return None

        try:
            for packet in self.codec.parse(encoded_data):
                for frame in self.codec.decode(packet):
                    return frame.to_ndarray(format="bgr24")
        except av.error.InvalidDataError:
            return None

        return None

class ComputeNode:
    def __init__(self, model):
        self.model = model
        self.inference_lock = threading.Lock()
        self.decoders = {}
        self.decoders_lock = threading.Lock()

        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:
            self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def cleanup_loop(self):
        while True:
            time.sleep(CLEANUP_INTERVAL)
            now = time.time()
            with self.decoders_lock:
                stale_streams = [
                    stream_id
                    for stream_id, decoder in self.decoders.items()
                    if now - decoder.last_active > TIMEOUT_SECONDS
                ]
                for stream_id in stale_streams:
                    print(f"Cleaning up stale decoder for stream {stream_id}")
                    del self.decoders[stream_id]

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker with result code {rc}")
        client.subscribe("compute/frames/+")

    def on_message(self, client, userdata, msg):
        topic_parts = msg.topic.split("/")
        if len(topic_parts) != 3:
            return
        stream_id = topic_parts[2]

        try:
            encoded_frame = fw_pb2.EncodedFrame.FromString(msg.payload)
        except Exception as e:
            print(f"Failed to unmarshal EncodedFrame: {e}")
            return

        task_id = encoded_frame.task_id
        mime_type = encoded_frame.mime_type

        telemetry.transmit_measure_event("compute3_frame_received", task_id)

        try:
            sample = fw_pb2.MediaSample.FromString(encoded_frame.data)
        except Exception as e:
            print(f"Failed to unmarshal MediaSample protobuf for task {task_id}: {e}")
            return

        codec_name = get_codec_name(mime_type)

        with self.decoders_lock:
            if stream_id not in self.decoders:
                print(f"New stream detected: {stream_id}")
                self.decoders[stream_id] = VideoDecoder()
            decoder = self.decoders[stream_id]

        try:
            decoder.configure(codec_name)
        except ValueError as err:
            print(err)
            return

        frame = decoder.decode(sample.data)
        if frame is None:
            return

        telemetry.transmit_measure_event("compute3_frame_decoded", task_id)

        with self.inference_lock:
            results = self.model(frame, stream=True, conf=0.5, verbose=False)

        telemetry.transmit_measure_event("compute3_inference_complete", task_id)

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

        inference_result = fw_pb2.InferenceResult(
            task_id=task_id,
            processing_status=0,
            detections=grpc_boxes
        )

        result_topic = f"compute/results/{stream_id}"
        self.client.publish(result_topic, inference_result.SerializeToString())
        telemetry.transmit_measure_event("compute3_result_yielded", task_id)

    def start(self):
        print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_forever()

async def register_app():
    app_id = os.getenv("APP_ID")
    comp_id = os.getenv("COMPONENT_ID")
    if not app_id or not comp_id:
        return

    node_id = os.getenv("NODE_ID", socket.gethostname())
    nats_url = os.getenv("CENTRAL_NATS_URL", "nats://localhost:4222")

    try:
        nc = await nats.connect(nats_url)
        req = engine_pb2.AppRegistrationRequest(
            app_id=app_id,
            component_id=comp_id,
            node_id=node_id
        )
        msg = await nc.request("c2.engine.app_register", req.SerializeToString(), timeout=2)
        resp = engine_pb2.AppRegistrationResponse()
        resp.ParseFromString(msg.data)
        print(f"App component registered with Central Engine: success={resp.success}")
        await nc.close()
    except Exception as e:
        print(f"Failed to register app component with Central Engine: {e}")

def serve():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(register_app())
    except Exception as e:
        print(f"Failed to run register_app: {e}")

    print(f"Loading TensorRT model: {MODEL_PATH}...")
    trt_model = YOLO(MODEL_PATH, task="detect")

    node = ComputeNode(trt_model)
    try:
        node.start()
    except KeyboardInterrupt:
        print("\nShutting down compute node cleanly.")

if __name__ == "__main__":
    serve()
