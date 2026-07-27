import asyncio
import threading
import time
from google.protobuf.timestamp_pb2 import Timestamp

import proto.fw_pb2 as fw_pb2
try:
    import nats
except ImportError:
    nats = None
    print("Warning: nats-py is not installed. Telemetry will be disabled. Install with `pip install nats-py`")

class TelemetryClient:
    def __init__(self, nats_url="nats://10.119.207.216:4222", service_name="compute2"):
        self.nats_url = nats_url
        self.service_name = service_name
        self.loop = asyncio.new_event_loop()
        self.nc = None
        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

        # Schedule the connection in the loop
        if nats:
            asyncio.run_coroutine_threadsafe(self._connect(), self.loop)

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _connect(self):
        try:
            self.nc = await nats.connect(self.nats_url)
            print(f"TelemetryClient connected to {self.nats_url}")
        except Exception as e:
            print(f"TelemetryClient failed to connect to NATS: {e}")

    async def _publish(self, event_type, task_id, user_id, timestamp):
        if not self.nc:
            return

        meas_time = Timestamp()
        meas_time.FromSeconds(int(timestamp))
        meas_time.nanos = int((timestamp - int(timestamp)) * 1e9)

        event = fw_pb2.MeasureEvent(
            meas_time=meas_time,
            user_id=user_id,
            task_id=task_id,
            service_name=self.service_name,
            event_type=event_type,
            payload={}
        )
        try:
            await self.nc.publish("events.measure", event.SerializeToString())
        except Exception as e:
            print(f"Failed to publish telemetry event: {e}")

    def transmit_measure_event(self, event_type, task_id, user_id=""):
        if not nats:
            return

        timestamp = time.time()
        asyncio.run_coroutine_threadsafe(
            self._publish(event_type, task_id, user_id, timestamp),
            self.loop
        )

# Global singleton
telemetry = TelemetryClient()
