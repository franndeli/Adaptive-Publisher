import time
import threading
from collections import deque
from typing import Deque, List, Tuple, Optional

from opentracing.propagation import Format

from adaptive_publisher.event_publishers.publisher import EventPublisher
import datetime
import uuid


class MicroBatchingEventPublisher(EventPublisher):
    """
    Micro-batching publisher: groups N frames into ONE event and sends it once.

    Flush triggers:
      - fixed batch size reached (primary goal)
      - timeout reached since first frame in batch (safety/latency bound)

    Preserves:
      - tracing extract + span creation
      - write_event_with_trace usage
    """

    def __init__(
        self,
        parent_service,
        publisher_details,
        query_ids,
        buffer_stream_key,
        batch_size: int = 10,
        batch_timeout: float = 0.1,
    ):
        super().__init__(parent_service, publisher_details, query_ids, buffer_stream_key)

        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

        # Each item: (frame, frame_index, trace_id)
        self._batch: Deque[Tuple[object, int, str]] = deque()
        self._first_time: Optional[float] = None

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._stop = False

        # background flusher to enforce timeout even if no new frames arrive
        self._flusher = threading.Thread(target=self._flush_worker, daemon=True)
        self._flusher.start()

    def generate_and_send_event(self, frame, frame_index, trace_id):
        """
        Called per frame by the service.

        We only enqueue quickly here. Heavy work (uploads + write) happens outside locks.
        """
        batch_to_send = None

        with self._cv:
            if not self._batch:
                self._first_time = time.time()

            self._batch.append((frame, frame_index, trace_id))

            # Fixed-size flush (your main goal)
            if len(self._batch) >= self.batch_size:
                batch_to_send = self._drain_locked()

            # Wake flusher thread (so it can update its timeout wait)
            self._cv.notify()

        # Do slow work outside the lock
        if batch_to_send:
            self._send_batched_event(batch_to_send)

    def flush(self):
        """Force flush pending frames immediately."""
        batch_to_send = None
        with self._cv:
            batch_to_send = self._drain_locked()
            self._cv.notify()

        if batch_to_send:
            self._send_batched_event(batch_to_send)

    def close(self):
        """
        Optional: call when shutting down to flush & stop thread.
        If your service doesn't call it, that's okay—daemon thread won't block exit.
        """
        with self._cv:
            self._stop = True
            self._cv.notify()
        self.flush()

    # ------------------------
    # Internal helpers
    # ------------------------

    def _flush_worker(self):
        """
        Ensures timeout-based flushing works even when frames stop arriving.
        """
        while True:
            batch_to_send = None

            with self._cv:
                # wait for frames or stop
                while not self._batch and not self._stop:
                    self._cv.wait()

                if self._stop:
                    # drain everything on stop
                    batch_to_send = self._drain_locked()
                    # exit after sending
                    break

                # Wait until timeout expires, unless size flush happens earlier.
                while self._batch:
                    assert self._first_time is not None
                    elapsed = time.time() - self._first_time
                    remaining = self.batch_timeout - elapsed

                    if remaining <= 0:
                        batch_to_send = self._drain_locked()
                        break

                    # If we got enough frames while waiting, drain now
                    if len(self._batch) >= self.batch_size:
                        batch_to_send = self._drain_locked()
                        break

                    # sleep until either notified (new frames) or timeout
                    self._cv.wait(timeout=remaining)

            # send outside lock
            if batch_to_send:
                self._send_batched_event(batch_to_send)

        # final send outside lock (on stop)
        if batch_to_send:
            self._send_batched_event(batch_to_send)

    def _drain_locked(self) -> List[Tuple[object, int, str]]:
        """Drain all queued frames (must be called under self._cv lock)."""
        if not self._batch:
            self._first_time = None
            return []

        items = list(self._batch)
        self._batch.clear()
        self._first_time = None
        return items

    def _send_batched_event(self, items: List[Tuple[object, int, str]]):
        """
        Creates ONE batched event and sends it with write_event_with_trace.

        Preserves tracing logic:
          span_ctx = tracer.extract(...)
          with tracer.start_active_span(..., child_of=span_ctx)
        """
        if not items:
            return

        # Use last trace id for "preserve tracing" semantics (similar to your version)
        last_trace_id = items[0][2]
        span_ctx = self.tracer.extract(Format.HTTP_HEADERS, {"uber-trace-id": last_trace_id})

        with self.tracer.start_active_span("generate_and_send_event", child_of=span_ctx) as scope:
            # Generate per-frame event_data (re-uses your existing logic / uploads)
            frames_data = []
            first_frame_index = items[0][1]
            last_frame_index = items[-1][1]

            for frame, frame_index, _trace in items:
                # Reuse original per-frame behavior
                event_data = self.generate_event_from_frame(frame, frame_index)
                # Keep only what you want downstream; or include full event_data if needed
                frames_data.append(
                    {
                        "frame_index": event_data["frame_index"],
                        "image_url": event_data["image_url"],
                        "timestamp": event_data["timestamp"],
                    }
                )

            # Build ONE event that contains multiple frames
            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
            event_id = f'{self.publisher_details["publisher_id"]}-batch-{str(uuid.uuid4())}'

            batched_event_data = {
                "id": event_id,
                "publisher_id": self.publisher_details["publisher_id"],
                "source": self.publisher_details["source"],
                "vekg": {},
                "query_ids": self.query_ids,
                "width": self.width,
                "height": self.height,
                "color_channels": self.color_channels,
                "timestamp": timestamp,
                "batch_size": len(items),
                "first_frame_index": first_frame_index,
                "last_frame_index": last_frame_index,
                "frames": frames_data,
            }

            # ✅ Preserve "actual data transmission" line (same method call)
            self.parent_service.write_event_with_trace(batched_event_data, self.bufferstream)

            self.logger.info(
                f'Sent batched event ({len(items)} frames: {first_frame_index}-{last_frame_index}) '
                f'to buffer stream: "{self.buffer_stream_key}" | '
            )
