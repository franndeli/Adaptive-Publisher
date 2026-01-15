import threading
import datetime
import statistics

import cv2
from opentracing.propagation import Format
from event_service_utils.services.tracer import EVENT_ID_TAG

import uuid
import os
import time

from adaptive_publisher.conf import (
    PUBLISHER_FPS,
    PUBLISHER_HEIGHT,
    PUBLISHER_ID,
    PUBLISHER_INPUT_SOURCE,
    PUBLISHER_WIDTH,
    REDIS_ADDRESS,
    REDIS_PORT,
    REDIS_EXPIRATION_TIME,
    PUB_EVENT_LIST,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    EVENT_GENERATOR_TYPE,
    EARLY_FILTERING_PIPELINE_NAME,
    LOGGING_LEVEL,
    TRACER_REPORTING_HOST,
    TRACER_REPORTING_PORT,
    SERVICE_DETAILS,
    REDUCE_SCALE,
)


class EventPublisher():
    def __init__(self, parent_service, publisher_details, query_ids, buffer_stream_key):
        self.parent_service = parent_service
        self.file_storage_cli = self.parent_service.file_storage_cli
        self.tracer = self.parent_service.tracer
        self.logger = self.parent_service.logger
        self.publisher_details = publisher_details

        self.width, self.height = self.publisher_details['meta']['resolution'].split('x')
        self.width = int(self.width)
        self.height = int(self.height)
        self.color_channels = 'BGR'
        self.query_ids = query_ids

        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.frame_ready = False
        self.frame_sent = True

        self.buffer_stream_key = buffer_stream_key
        self.bufferstream = self.parent_service.stream_factory.create(self.buffer_stream_key, stype='streamOnly')
        self.store_sizes = []

    def run(self):
        with self.condition:
            while not self.frame_ready:
                self.condition.wait()

            frame, frame_index, trace_id = self.frame_data
            self.frame_ready = False

        self.generate_and_send_event(frame, frame_index, trace_id)
        with self.condition:
            self.frame_sent = True
            self.condition.notify()

    def generate_and_send_event(self, frame, frame_index, trace_id):
        span_ctx = self.tracer.extract(Format.HTTP_HEADERS, {'uber-trace-id': trace_id})
        with self.tracer.start_active_span('generate_and_send_event', child_of=span_ctx) as scope:
            event_data = self.generate_event_from_frame(frame, frame_index, trace_id)

            self.parent_service.write_event_with_trace(event_data, self.bufferstream)
            self.logger.info(f'sending event_data "{event_data}", to buffer stream: "{self.buffer_stream_key}"')

    def generate_event_from_frame(self, frame, frame_index, trace_id=None):
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        event_id = f'{self.publisher_details["publisher_id"]}-{uuid.uuid4()}'

        # -------------------------
        # Keep original behavior: optional downscale + expiration tweaks
        # -------------------------
        if REDUCE_SCALE > 1:
            frame = cv2.resize(
                frame,
                (self.width // REDUCE_SCALE, self.height // REDUCE_SCALE),
                interpolation=cv2.INTER_LINEAR
            )

            # original expiration tweaks (keep as-is)
            if frame_index < 1600:
                self.file_storage_cli.expiration_time = 240
            if frame_index < 328:
                self.file_storage_cli.expiration_time = 60

        # -------------------------
        # Block comparison: baseline -> jpegxl_lossless -> jpegls_lossless
        # -------------------------
        compare_blocks = os.getenv("COMPARE_BLOCKS", "1").strip().lower() in ("1", "true", "yes", "on")
        block_size = int(os.getenv("COMPARE_BLOCK_SIZE", "50"))

        if compare_blocks:
            block = (frame_index // block_size) % 3
            if block == 0:
                image_mode = "baseline"
            elif block == 1:
                image_mode = "jpegxl_lossless"
            else:
                image_mode = "jpegls_lossless"
        else:
            image_mode = os.getenv("IMAGE_MODE", "baseline").strip().lower()

        # JPEG-XL params (lossless)
        jxl_effort = int(os.getenv("JXL_EFFORT", "7"))  # 1..9 (higher => slower, smalle
        # JPEG-LS params (near=0 means lossless; some builds may not expose 'near')
        jls_near = int(os.getenv("JLS_NEAR", "0"))

        # Jaeger/OpenTracing tracer
        tracer = getattr(self, "tracer", None) or getattr(getattr(self, "parent_service", None), "tracer", None)

        def _event(img_uri: str, payload_bytes: int = None):
            ev = {
                'id': event_id,
                'publisher_id': self.publisher_details['publisher_id'],
                'source': self.publisher_details['source'],
                'image_url': img_uri,
                'vekg': {},
                'query_ids': self.query_ids,
                'width': self.width,
                'height': self.height,
                'color_channels': self.color_channels,
                'frame_index': frame_index,
                'timestamp': timestamp,
                'image_mode': image_mode,
            }
            if payload_bytes is not None:
                ev['payload_bytes'] = payload_bytes
            return ev

        span = None
        if tracer is not None:
            span = tracer.start_span("encode_and_store_frame")
            span.set_tag("frame.index", frame_index)
            span.set_tag("image.mode", image_mode)
            span.set_tag("compare.block_size", block_size)
            span.set_tag("reduce.scale", int(REDUCE_SCALE))
            if compare_blocks:
                span.set_tag("compare.block_index", frame_index // block_size)

        try:
            # -------------------------
            # BASELINE (existing path)
            # -------------------------
            if image_mode == "baseline":
                if span:
                    span.set_tag("image.lossless", False)
                    span.set_tag("image.codec", "baseline")

                t0 = time.time()
                img_uri = self.file_storage_cli.upload_inmemory_to_storage(frame)
                t1 = time.time()

                if span:
                    span.set_tag("baseline.upload.ms", (t1 - t0) * 1000.0)
                    span.set_tag("payload.bytes", int(getattr(frame, "nbytes", 0)))

                return _event(img_uri, payload_bytes=int(getattr(frame, "nbytes", 0)))

            # -------------------------
            # JPEG-XL (lossless)
            # -------------------------
            elif image_mode == "jpegxl_lossless":
                if span:
                    span.set_tag("image.lossless", True)
                    span.set_tag("image.codec", "jpegxl")
                    span.set_tag("jxl.level", 0)
                    span.set_tag("jxl.effort", jxl_effort)

                try:
                    import imagecodecs
                except Exception as e:
                    raise RuntimeError("JPEG-XL selected but imagecodecs is not importable.") from e

                if not hasattr(imagecodecs, "jpegxl_encode"):
                    raise RuntimeError("imagecodecs.jpegxl_encode not available (no JPEG-XL support).")

                enc0 = time.time()
                img_rgb = frame[:, :, ::-1]  # BGR -> RGB
                jxl_bytes = imagecodecs.jpegxl_encode(img_rgb, level=0, effort=jxl_effort)
                jxl_bytes = bytes(jxl_bytes)

                enc1 = time.time()

                payload_bytes = len(jxl_bytes)

                up0 = time.time()
                img_uri = str(uuid.uuid4())
                self.file_storage_cli.client.set(
                    name=img_uri,
                    value=jxl_bytes,
                    ex=self.file_storage_cli.expiration_time,
                )
                up1 = time.time()

                if span:
                    span.set_tag("encode.ms", (enc1 - enc0) * 1000.0)
                    span.set_tag("upload.ms", (up1 - up0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)

                return _event(img_uri, payload_bytes=payload_bytes)

            # -------------------------
            # JPEG-LS (lossless)
            # -------------------------
            elif image_mode == "jpegls_lossless":
                if span:
                    span.set_tag("image.lossless", True)
                    span.set_tag("image.codec", "jpegls")
                    span.set_tag("jls.near", jls_near)

                try:
                    import imagecodecs
                except Exception as e:
                    raise RuntimeError("JPEG-LS selected but imagecodecs is not importable.") from e

                if not hasattr(imagecodecs, "jpegls_encode"):
                    raise RuntimeError("imagecodecs.jpegls_encode not available (no JPEG-LS support).")

                enc0 = time.time()
                img_rgb = frame[:, :, ::-1]  # BGR -> RGB

                # Some builds support `near=...`, others don't — handle both.
                try:
                    jls_bytes = imagecodecs.jpegls_encode(img_rgb, near=jls_near)
                    jls_bytes = bytes(jls_bytes)

                except TypeError:
                    jls_bytes = imagecodecs.jpegls_encode(img_rgb)

                enc1 = time.time()

                payload_bytes = len(jls_bytes)

                up0 = time.time()
                img_uri = str(uuid.uuid4())
                self.file_storage_cli.client.set(
                    name=img_uri,
                    value=jls_bytes,
                    ex=self.file_storage_cli.expiration_time,
                )
                up1 = time.time()

                if span:
                    span.set_tag("encode.ms", (enc1 - enc0) * 1000.0)
                    span.set_tag("upload.ms", (up1 - up0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)

                return _event(img_uri, payload_bytes=payload_bytes)

            else:
                raise ValueError(f"Unknown image_mode: {image_mode}")

        finally:
            if span is not None:
                span.finish()


# In jaeger UI you can use the tags to filter:
# compare.block_index=0 image.mode=baseline
# compare.block_index=1 image.mode=jpegxl_lossless
# compare.block_index=2 image.mode=jpegls_lossless
