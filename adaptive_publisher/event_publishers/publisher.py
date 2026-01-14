import threading
import datetime
import statistics

import cv2
from opentracing.propagation import Format

from event_service_utils.services.tracer import EVENT_ID_TAG
import uuid


import os
import time
import uuid

import cv2

import lz4.frame as lz4


# When you want to use QOI don't forget to uncomment the import line below:

# import qoi

try:
    from opentelemetry import trace
except Exception:
    trace = None

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

    # def run_forever(self):
    #     while True:
    #         with self.condition:
    #             while not self.frame_ready:
    #                 self.condition.wait()

    #             frame, frame_index, trace_id = self.frame_data
    #             self.frame_ready = False

    #         self.generate_and_send_event(frame, frame_index, trace_id)
    #         with self.condition:
    #             self.frame_sent = True
    #             self.condition.notify()

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

    # def generate_event_from_frame(self, frame, frame_index, trace_id=None):
    #     # Get current UTC timestamp
    #     timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

    #     event_id = f'{self.publisher_details["publisher_id"]}-{str(uuid.uuid4())}'

    #     if REDUCE_SCALE > 1:
    #         frame = cv2.resize(frame, (self.width // REDUCE_SCALE, self.height // REDUCE_SCALE), interpolation=cv2.INTER_LINEAR)
    #         # workaround to the absurd ammount of mem usage in the server
    #         # by the cloudseg method, so try to make some space available as soon as
    #         # it can be made
    #         if frame_index < 1600:
    #             self.file_storage_cli.expiration_time = 240
    #         if frame_index < 328:
    #             self.file_storage_cli.expiration_time = 60

    #     img_uri = self.file_storage_cli.upload_inmemory_to_storage(frame)


    #     # store_size = getsizeof(frame.tobytes(order='C'))
    #     # store_size = self.file_storage_cli.client.execute_command(f'MEMORY USAGE {img_uri}')
    #     # self.store_sizes.append(int(store_size))
    #     # std = 0
    #     # if len(self.store_sizes) > 1:
    #     #     std = statistics.stdev(self.store_sizes)
    #     # self.parent_service.logger.error(f'>>> total ({len(self.store_sizes)}) last_size: {store_size} >> avg sizes: {sum(self.store_sizes) / len(self.store_sizes)} | std: {std}')
    #     event_data = {
    #         'id': event_id,
    #         'publisher_id': self.publisher_details['publisher_id'],
    #         'source': self.publisher_details['source'],
    #         'image_url': img_uri,
    #         'vekg': {},
    #         'query_ids': self.query_ids,
    #         'width': self.width,
    #         'height': self.height,
    #         'color_channels': self.color_channels,
    #         'frame_index': frame_index,
    #         'timestamp': timestamp,
    #     }
    #     return event_data


   
    def generate_event_from_frame(self, frame, frame_index, trace_id=None):
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        event_id = f'{self.publisher_details["publisher_id"]}-{uuid.uuid4()}'

        # ---- Choose ONE mode for the whole run/video ----
        # If IMAGE_MODE is set -> always use that (no block switching)
        # Else, optionally fall back to compare blocks when enabled.
        image_mode_env = os.getenv("IMAGE_MODE", "").strip().lower()
        compare_blocks = os.getenv("COMPARE_BLOCKS", "0").strip().lower() in ("1", "true", "yes", "on")
        block_size = int(os.getenv("COMPARE_BLOCK_SIZE", "50"))

        if image_mode_env:
            image_mode = image_mode_env
            compare_blocks = False  # force off, so it stays constant for the whole video/run
        elif compare_blocks:
            block = (frame_index // block_size) % 6
            if block == 0:
                image_mode = "baseline"
            elif block == 1:
                image_mode = "png_lossless"
            elif block == 2:
                image_mode = "webp_lossless"
            elif block == 3:
                image_mode = "lz4_lossless"
            elif block == 4:
                image_mode = "qoi"
            else:
                image_mode = "JPEGXL"
        else:
            image_mode = "webp_lossless"

        # PNG compression: 0 fastest (still lossless), 9 smallest (slow)
        png_level = int(os.getenv("PNG_COMPRESSION_LEVEL", "0"))

        # WebP settings
        webp_quality = int(os.getenv("WEBP_QUALITY", "100"))
        webp_lossless = os.getenv("WEBP_LOSSLESS", "1").strip().lower() in ("1", "true", "yes", "on")
        
        lz4_param = [lz4.BLOCKSIZE_MAX4MB, lz4.COMPRESSIONLEVEL_MIN]

        # LZ4 settings
        # lz4_level = int(os.getenv("LZ4_LEVEL", "0"))

        tracer = getattr(self, "tracer", None) or getattr(getattr(self, "parent_service", None), "tracer", None)

        def _event(img_uri: str):
            return {
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

        span = None
        if tracer is not None:
            span = tracer.start_span("encode_and_store_frame")
            span.set_tag("frame.index", frame_index)
            span.set_tag("image.mode", image_mode)
            span.set_tag("compare.block_size", block_size)
            if compare_blocks:
                span.set_tag("compare.block_index", frame_index // block_size)

        try:
            if image_mode == "baseline":
                if span:
                    span.set_tag("image.lossless", False)
                    span.set_tag("image.codec", "baseline")

                t0 = time.time()
                img_uri = self.file_storage_cli.upload_inmemory_to_storage(frame)
                t1 = time.time()

                payload_bytes = frame.nbytes

                if span:
                    span.set_tag("baseline.upload.ms", (t1 - t0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)
                   

                return _event(img_uri)

            elif image_mode == "png_lossless":
                if span:
                    span.set_tag("image.lossless", True)
                    span.set_tag("image.codec", "png")
                    span.set_tag("png.compression_level", png_level)

                enc0 = time.time()
                ok, buffer = cv2.imencode(".png", frame, [cv2.IMWRITE_PNG_COMPRESSION, png_level])
                enc1 = time.time()
                if not ok:
                    raise RuntimeError("PNG encoding failed")

                png_bytes = buffer.tobytes()
                payload_bytes = len(png_bytes)

                up0 = time.time()
                img_uri = str(uuid.uuid4())
                self.file_storage_cli.client.set(
                    name=img_uri,
                    value=png_bytes,
                    ex=self.file_storage_cli.expiration_time,
                )
                up1 = time.time()

                if span:
                    span.set_tag("encode.ms", (enc1 - enc0) * 1000.0)
                    span.set_tag("upload.ms", (up1 - up0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)

                return _event(img_uri)

            elif image_mode == "webp_lossless":
                from PIL import Image
                import io

                if span:
                    span.set_tag("image.lossless", True)
                    span.set_tag("image.codec", "webp")
                    span.set_tag("webp.lossless", bool(webp_lossless))
                    span.set_tag("webp.method", 0)
                    span.set_tag("webp.quality", webp_quality)

                enc0 = time.time()
                img_rgb = frame[:, :, ::-1]  # BGR -> RGB

                buf = io.BytesIO()
                Image.fromarray(img_rgb).save(
                    buf,
                    format="WEBP",
                    lossless=True if webp_lossless else False,
                    quality=webp_quality,
                    method=0,
                )

                webp_bytes = buf.getvalue()
                enc1 = time.time()

                payload_bytes = len(webp_bytes)

                up0 = time.time()
                img_uri = str(uuid.uuid4())
                self.file_storage_cli.client.set(
                    name=img_uri,
                    value=webp_bytes,
                    ex=self.file_storage_cli.expiration_time,
                )
                up1 = time.time()

                if span:
                    span.set_tag("encode.ms", (enc1 - enc0) * 1000.0)
                    span.set_tag("upload.ms", (up1 - up0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)

                return _event(img_uri)

            elif image_mode == "lz4_lossless":

                if span:
                    span.set_tag("image.lossless", True)
                    span.set_tag("image.codec", "lz4")
                    span.set_tag("lz4.blocksize", lz4_param[0])
                    span.set_tag("lz4.comp", lz4_param[1])

                enc0 = time.time()

                lz4_bytes = lz4.compress(
                    frame,
                    block_size=lz4_param[0],
                    compression_level=lz4_param[1]
                )

                enc1 = time.time()

                payload_bytes = len(lz4_bytes)

                up0 = time.time()
                img_uri = str(uuid.uuid4())
                self.file_storage_cli.client.set(
                    name=img_uri,
                    value=lz4_bytes,
                    ex=self.file_storage_cli.expiration_time,
                )
                up1 = time.time()

                if span:
                    span.set_tag("encode.ms", (enc1 - enc0) * 1000.0)
                    span.set_tag("upload.ms", (up1 - up0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)

                return _event(img_uri)
            
            # -------------------------
            # QOI
            # -------------------------
            elif image_mode == "qoi":

                if span:
                    span.set_tag("image.lossless", True)
                    span.set_tag("image.codec", "qoi")

                enc0 = time.time()

                qoi_bytes = qoi.encode(
                    frame
                )

                enc1 = time.time()

                payload_bytes = len(qoi_bytes)

                up0 = time.time()
                img_uri = str(uuid.uuid4())
                self.file_storage_cli.client.set(
                    name=img_uri,
                    value=qoi_bytes,
                    ex=self.file_storage_cli.expiration_time,
                )
                up1 = time.time()

                if span:
                    span.set_tag("encode.ms", (enc1 - enc0) * 1000.0)
                    span.set_tag("upload.ms", (up1 - up0) * 1000.0)
                    span.set_tag("payload.bytes", payload_bytes)

                return _event(img_uri)

            else:
                raise ValueError(f"Unknown image_mode: {image_mode}")

        finally:
            if span is not None:
                span.finish()



# In jaeger UI you can use the tags to filter for baseline/png/webp images
# compare.block_index=0 image.mode=baseline
# compare.block_index=1 image.mode=png_lossless
# compare.block_index=2 image.mode=webp_lossless

