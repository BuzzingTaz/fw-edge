"""
====================================================================
PYTHON COMPUTE WORKER
====================================================================

ARCHITECTURE OVERVIEW
====================================================================

OLD ARCHITECTURE
--------------------------------------------------------------------

Browser
   ↓
Scheduler Go
   ↓ UDP RTP packets
Python Worker
   ↓ GStreamer udpsrc
OpenCV VideoCapture(cap.read())
   ↓
YOLO
   ↓
Python gRPC CLIENT
   ↓
Go Compute Server

Problems:
- Required extra Go compute bridge
- Required second proto file (inference.proto)
- Required shared inference cache in Go
- Required localhost UDP forwarding
- More moving parts

====================================================================

NEW ARCHITECTURE
--------------------------------------------------------------------

Browser
   ↓
Scheduler Go
   ↓ gRPC bidi stream (RTP packets)
Python Worker (THIS FILE)
   ↓ appsrc (instead of udpsrc)
GStreamer decode
   ↓
YOLO
   ↓
yield InferenceResult directly over same gRPC stream

Advantages:
- Single proto file
- No compute bridge
- No shared cache
- Cleaner architecture
- Easier scaling
- Scheduler stays lightweight
- Python workers become independently scalable

====================================================================

IMPORTANT CHANGE FROM OLD CODE
====================================================================

OLD:
-----
GStreamer itself received UDP packets using:

    udpsrc port=5000

NEW:
-----
Python receives RTP packets from gRPC manually.

So now Python itself pushes packets INTO GStreamer using:

    appsrc

That is the main architectural difference.

====================================================================
"""

import time
import queue
import threading
from concurrent import futures

import grpc
import gi
import numpy as np
from ultralytics import YOLO

# Generated from fw.proto (fw.proto is sitting next to this file in the same directory.)
import fw_pb2
import fw_pb2_grpc

# -------------------------------------------------------------------
# GStreamer Setup
# -------------------------------------------------------------------
#
# We use GStreamer because:
#
# - RTP packet handling is complex
# - VP8 depacketization is complex
# - Hardware decoding is important on Jetson
# - GStreamer already solves all of this efficiently
#
# -------------------------------------------------------------------

gi.require_version("Gst", "1.0")
from gi.repository import Gst

# Initialize GStreamer globally
Gst.init(None)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

# gRPC server port
GRPC_PORT = 9997

# TensorRT engine
MODEL_PATH = "yolov8n.engine"

# Maximum decoded frames buffered
#
# Prevents unlimited memory growth if inference
# becomes slower than incoming video stream.
FRAME_QUEUE_SIZE = 30

# YOLO confidence threshold
CONFIDENCE_THRESHOLD = 0.5


# ===================================================================
# Compute Worker Service
# ===================================================================
#
# This implements:
#
# service ComputeStream {
#     rpc StreamVideo(stream RTPPacket)
#         returns (stream InferenceResult);
# }
#
# Each StreamVideo() call represents ONE CLIENT SESSION.
#
# The scheduler creates one gRPC stream per websocket client.
#
# ===================================================================

class ComputeWorker(
    fw_pb2_grpc.ComputeStreamServicer
):

    def __init__(self):

        print(
            f"[INIT] Loading TensorRT model: {MODEL_PATH}"
        )

        # ------------------------------------------------------------
        # Load YOLO TensorRT model once at startup
        #
        # This avoids reloading model per client.
        # ------------------------------------------------------------
        self.model = YOLO(
            MODEL_PATH,
            task="detect",
        )

        print("[INIT] Model loaded successfully")

    # ===================================================================
    # StreamVideo()
    # ===================================================================
    #
    # Scheduler streams RTP packets INTO this function.
    #
    # We stream inference results BACK to scheduler.
    #
    # Bidirectional stream:
    #
    # Scheduler ---> RTP packets
    # Worker    ---> Inference results
    #
    # ===================================================================

    def StreamVideo(
        self,
        request_iterator,
        context,
    ):

        print(
            "[STREAM] New client stream connected"
        )

        # ----------------------------------------------------------------
        # Frame Queue
        # ----------------------------------------------------------------
        #
        # OLD CODE:
        # ----------
        # cap.read() directly returned decoded frames.
        #
        # NEW CODE:
        # ----------
        # GStreamer appsink callback pushes frames into this queue.
        #
        # YOLO inference loop consumes from this queue.
        #
        # This decouples:
        # - decoding thread
        # - inference thread
        #
        # ----------------------------------------------------------------
        frame_queue = queue.Queue(
            maxsize=FRAME_QUEUE_SIZE
        )

        # ===================================================================
        # GStreamer Pipeline
        # ===================================================================
        #
        # OLD PIPELINE:
        # -------------
        #
        # udpsrc port=5000 !
        #
        # GStreamer itself owned UDP socket.
        #
        # ===================================================================
        #
        # NEW PIPELINE:
        # -------------
        #
        # appsrc name=src !
        #
        # Python manually feeds RTP packets into GStreamer.
        #
        # ===================================================================

        pipeline_str = """
        appsrc name=src is-live=true format=time do-timestamp=true !
        application/x-rtp,media=video,encoding-name=VP8,payload=96 !
        rtpjitterbuffer latency=50 !
        rtpvp8depay !
        nvv4l2decoder !
        nvvidconv !
        video/x-raw,format=BGRx !
        videoconvert !
        video/x-raw,format=BGR !
        appsink name=sink emit-signals=true sync=false drop=true max-buffers=1
        """

        print("[GST] Creating pipeline")

        pipeline = Gst.parse_launch(
            pipeline_str
        )

        # ----------------------------------------------------------------
        # appsrc
        # ----------------------------------------------------------------
        #
        # Python pushes RTP packets HERE.
        #
        # Replaces old:
        #
        #     udpsrc
        #
        # ----------------------------------------------------------------
        appsrc = pipeline.get_by_name("src")

        # ----------------------------------------------------------------
        # appsink
        # ----------------------------------------------------------------
        #
        # Decoded frames come OUT here.
        #
        # Replaces old:
        #
        #     cap.read()
        #
        # ----------------------------------------------------------------
        appsink = pipeline.get_by_name("sink")

        # ===================================================================
        # appsink callback
        # ===================================================================
        #
        # Called every time GStreamer decodes a frame.
        #
        # OLD CODE:
        # ----------
        # ret, frame = cap.read()
        #
        # NEW CODE:
        # ----------
        # appsink callback provides raw frame buffer.
        #
        # ===================================================================

        def on_new_sample(sink):

            # Pull decoded frame sample
            sample = sink.emit("pull-sample")

            if sample is None:
                return Gst.FlowReturn.ERROR

            # Raw buffer from GStreamer
            buf = sample.get_buffer()

            # Video metadata
            caps = sample.get_caps()

            structure = caps.get_structure(0)

            width = structure.get_value("width")
            height = structure.get_value("height")

            # Map raw memory into Python
            success, map_info = buf.map(
                Gst.MapFlags.READ
            )

            if not success:
                print(
                    "[GST] Failed to map buffer"
                )
                return Gst.FlowReturn.ERROR

            try:

                # ------------------------------------------------------------
                # Convert raw bytes -> numpy array
                # ------------------------------------------------------------
                arr = np.frombuffer(
                    map_info.data,
                    dtype=np.uint8
                )

                # ------------------------------------------------------------
                # Reshape into image
                #
                # Final pipeline format is:
                #
                # video/x-raw,format=BGR
                #
                # So shape is:
                #   (height, width, 3)
                # ------------------------------------------------------------
                frame = arr.reshape(
                    (height, width, 3)
                )

                # ------------------------------------------------------------
                # Drop frames if inference falls behind
                #
                # This prevents:
                # - memory explosion
                # - increasing latency
                #
                # Real-time systems usually prefer dropping
                # old frames instead of buffering infinitely.
                # ------------------------------------------------------------
                if not frame_queue.full():

                    frame_queue.put(
                        frame.copy()
                    )

            except Exception as e:

                print(
                    f"[GST] Frame conversion error: {e}"
                )

            finally:

                # Always unmap memory
                buf.unmap(map_info)

            return Gst.FlowReturn.OK

        # Attach callback to appsink
        appsink.connect(
            "new-sample",
            on_new_sample
        )

        # ----------------------------------------------------------------
        # Start pipeline
        # ----------------------------------------------------------------
        print("[GST] Starting pipeline")

        pipeline.set_state(
            Gst.State.PLAYING
        )

        # ===================================================================
        # RTP Ingest Thread
        # ===================================================================
        #
        # OLD CODE:
        # ----------
        # GStreamer internally received RTP packets from UDP.
        #
        # NEW CODE:
        # ----------
        # gRPC request_iterator provides RTP packets manually.
        #
        # We push those packets into appsrc.
        #
        # ===================================================================

        def ingest_rtp():

            print(
                "[STREAM] RTP ingest thread started"
            )

            try:

                # ------------------------------------------------------------
                # Receive RTP packets from scheduler
                # ------------------------------------------------------------
                for msg in request_iterator:

                    # Serialized RTP bytes
                    data = msg.data

                    # --------------------------------------------------------
                    # Create GStreamer buffer
                    # --------------------------------------------------------
                    gst_buffer = Gst.Buffer.new_allocate(
                        None,
                        len(data),
                        None,
                    )

                    # Copy RTP bytes into buffer
                    gst_buffer.fill(0, data)

                    # --------------------------------------------------------
                    # Push RTP packet into GStreamer
                    #
                    # This replaces:
                    #
                    #     udpsrc
                    #
                    # --------------------------------------------------------
                    retval = appsrc.emit(
                        "push-buffer",
                        gst_buffer,
                    )

                    if retval != Gst.FlowReturn.OK:

                        print(
                            f"[GST] push-buffer failed: {retval}"
                        )

            except Exception as e:

                print(
                    f"[STREAM] RTP ingest error: {e}"
                )

            finally:

                print(
                    "[STREAM] RTP stream ended"
                )

                # Signal end-of-stream to GStreamer
                appsrc.emit("end-of-stream")

        # ----------------------------------------------------------------
        # Start ingest thread
        # ----------------------------------------------------------------
        ingest_thread = threading.Thread(
            target=ingest_rtp,
            daemon=True,
        )

        ingest_thread.start()

        # ===================================================================
        # Inference Loop
        # ===================================================================
        #
        # OLD CODE:
        # ----------
        #
        # while cap.isOpened():
        #     ret, frame = cap.read()
        #
        # NEW CODE:
        # ----------
        #
        # while stream active:
        #     frame = frame_queue.get()
        #
        # ===================================================================

        try:

            while context.is_active():

                try:

                    # --------------------------------------------------------
                    # Wait for decoded frame
                    #
                    # timeout prevents infinite blocking
                    # if stream disconnects unexpectedly.
                    # --------------------------------------------------------
                    frame = frame_queue.get(
                        timeout=1
                    )

                except queue.Empty:
                    continue

                # ------------------------------------------------------------
                # Run YOLO inference
                #
                # stream=True helps avoid memory buildup
                # during long-running inference loops.
                # ------------------------------------------------------------
                results = self.model(
                    frame,
                    stream=True,
                    verbose=False,
                    conf=CONFIDENCE_THRESHOLD,
                )

                detections = []

                # ------------------------------------------------------------
                # Convert YOLO detections -> protobuf
                # ------------------------------------------------------------
                for r in results:

                    if r.boxes is None:
                        continue

                    xywh = (
                        r.boxes.xywh
                        .cpu()
                        .numpy()
                    )

                    confs = (
                        r.boxes.conf
                        .cpu()
                        .numpy()
                    )

                    cls_ids = (
                        r.boxes.cls
                        .cpu()
                        .numpy()
                    )

                    for i in range(len(xywh)):

                        cls_id = int(
                            cls_ids[i]
                        )

                        # Resolve class label
                        label = str(
                            self.model.names.get(
                                cls_id,
                                str(cls_id),
                            )
                        )

                        detections.append(
                            fw_pb2.BoundingBox(
                                x=int(xywh[i][0]),
                                y=int(xywh[i][1]),
                                dx=int(xywh[i][2]),
                                dy=int(xywh[i][3]),
                                label=label,
                                confidence=float(
                                    confs[i]
                                ),
                            )
                        )

                # ------------------------------------------------------------
                # Send inference results back to scheduler
                #
                # IMPORTANT:
                # yield streams response incrementally
                # over the active gRPC stream.
                # ------------------------------------------------------------
                yield fw_pb2.InferenceResult(
                    timestamp=int(
                        time.time() * 1000
                    ),
                    processing_status=0,
                    detections=detections,
                )

        except Exception as e:

            print(
                f"[INFERENCE] Stream failed: {e}"
            )

        finally:

            print(
                "[STREAM] Cleaning up pipeline"
            )

            pipeline.set_state(
                Gst.State.NULL
            )


# ===================================================================
# gRPC Server Bootstrap
# ===================================================================

def serve():

    # ----------------------------------------------------------------
    # Create multithreaded gRPC server
    #
    # Each client stream may occupy a worker thread.
    # ----------------------------------------------------------------
    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=10
        )
    )

    # Register ComputeStream service
    fw_pb2_grpc.add_ComputeStreamServicer_to_server(
        ComputeWorker(),
        server,
    )

    # Listen on all interfaces
    server.add_insecure_port(
        f"[::]:{GRPC_PORT}"
    )

    server.start()

    print(
        f"[SERVER] Worker listening on :{GRPC_PORT}"
    )

    # Block forever
    server.wait_for_termination()


# ===================================================================
# Entry Point
# ===================================================================

if __name__ == "__main__":

    serve()