import socket
import struct
import threading
import queue
import os
import traceback

import av
from ultralytics import YOLO

import proto.fw_pb2 as fw_pb2

SOCKET_PATH = "/tmp/edge_compute_inference.sock"
MODEL_PATH = "yolo26n.engine"

# Matches Go sendEncodedFrameToPython:
# [packet_timestamp u32 BE][data_len u32 BE][codec u8][encoded frame bytes]
FRAME_HEADER = struct.Struct("!IIB")

CODEC_NAME = {
    0: "unknown",
    1: "vp8",
    2: "h264",
    3: "vp9",
    4: "hevc",
}


class VideoDecoder:
    def __init__(self):
        self.codec_name = None
        self.codec = None

    def configure(self, codec_id: int):
        codec_name = CODEC_NAME.get(codec_id, "unknown")
        if codec_name == "unknown":
            raise ValueError(f"unsupported codec id: {codec_id}")

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

def recvall(sock: socket.socket, n: int) -> bytearray:
    """Helper to read exactly n bytes from a stream socket."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

def uds_reader(conn: socket.socket, frame_queue: queue.Queue):
    """Continuously reads framed video packets and pushes them to a queue."""
    try:
        while True:
            header_data = recvall(conn, FRAME_HEADER.size)
            if not header_data:
                break

            packet_timestamp, data_len, codec_id = FRAME_HEADER.unpack_from(header_data)

            encoded_data = recvall(conn, data_len)
            if not encoded_data:
                break

            frame_queue.put((packet_timestamp, codec_id, encoded_data))
    except Exception as e:
        print(f"Reader thread terminated: {e}")
    finally:
        frame_queue.put(None)  # Sentinel to stop inference thread


def handle_client(conn: socket.socket, model: YOLO):
    """Processes a single client stream end-to-end."""
    print("New UDS client connected.")
    frame_queue = queue.Queue(maxsize=30)
    decoder = VideoDecoder()

    reader_thread = threading.Thread(target=uds_reader, args=(conn, frame_queue), daemon=True)
    reader_thread.start()

    try:
        while True:
            item = frame_queue.get()
            if item is None:
                break  # Client disconnected

            packet_timestamp, codec_id, encoded_data = item

            try:
                decoder.configure(codec_id)
            except ValueError as err:
                print(err)
                continue

            frame = decoder.decode(encoded_data)
            if frame is None:
                continue

            results = model(frame, stream=True, conf=0.5, verbose=False)
            grpc_boxes = []

            for r in results:
                if len(r.boxes) > 0:
                    xywh = r.boxes.xywh.cpu().numpy()
                    confs = r.boxes.conf.cpu().numpy()
                    cls_ids = r.boxes.cls.cpu().numpy()

                    for i in range(len(xywh)):
                        c_id = int(cls_ids[i].item())
                        if hasattr(model, "names") and isinstance(model.names, dict):
                            c_name = str(model.names.get(c_id, f"Class_{c_id}"))
                        else:
                            c_name = str(c_id)

                        grpc_boxes.append(
                            fw_pb2.BoundingBox(
                                label=c_name,
                                confidence=float(confs[i].item()),
                                x=int(xywh[i][0].item()),
                                y=int(xywh[i][1].item()),
                                dx=int(xywh[i][2].item()),
                                dy=int(xywh[i][3].item()),
                            )
                        )

            # Serialize and pack length-prefixed inference result
            result_msg = fw_pb2.InferenceResult(
                timestamp=int(packet_timestamp),
                detections=grpc_boxes
            )
            serialized = result_msg.SerializeToString()
            conn.sendall(struct.pack("!I", len(serialized)) + serialized)

    except Exception as e:
        print(f"Error handling UDS client: {e}")
        traceback.print_exc()
    finally:
        conn.close()
        print("UDS client disconnected.")


def run():
    print(f"Loading TensorRT model: {MODEL_PATH}...")
    trt_model = YOLO(MODEL_PATH, task="detect")

    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(SOCKET_PATH)
    server.listen()
    print(f"Listening for UDS connections on {SOCKET_PATH}")

    try:
        while True:
            conn, _ = server.accept()
            threading.Thread(target=handle_client, args=(conn, trt_model), daemon=True).start()
    except KeyboardInterrupt:
        print("Shutting down cleanly.")
    finally:
        server.close()
        if os.path.exists(SOCKET_PATH):
            os.remove(SOCKET_PATH)


if __name__ == "__main__":
    run()
