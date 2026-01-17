import threading
import datetime
import statistics

import cv2
from opentracing.propagation import Format

from event_service_utils.services.tracer import EVENT_ID_TAG
import uuid
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
    def __init__(self, parent_service, publisher_details, query_ids, buffer_stream_key, metrics_collector=None):
        self.parent_service = parent_service
        self.file_storage_cli = self.parent_service.file_storage_cli
        self.tracer = self.parent_service.tracer
        self.logger = self.parent_service.logger
        self.publisher_details = publisher_details
        self.metrics_collector = metrics_collector

        self.width, self.height = self.publisher_details['meta']['resolution'].split('x')
        self.width = int(self.width)
        self.height = int(self.height)
        self.color_channels = 'BGR'
        self.query_ids = query_ids

        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.frame_ready = False
        self.frame_sent = True
        self._stop_flag = False  # Flag to signal shutdown

        self.buffer_stream_key = buffer_stream_key
        self.bufferstream = self.parent_service.stream_factory.create(self.buffer_stream_key, stype='streamOnly')
        self.store_sizes = []

        self._frame_start_times = {}  # frame_index -> start_time
        self._throughput_measurements = []
        self._frame_read_timestamps = {}  # For latency tracking

    def record_frame_read(self, frame_index: int, read_timestamp: float):
        """Record when a frame was read (called from service for latency tracking)."""
        self._frame_read_timestamps[frame_index] = read_timestamp
        if self.metrics_collector:
            self.metrics_collector.record_frame_read(frame_index, read_timestamp)

    def stop(self):
        """Signal the publisher thread to stop."""
        with self.condition:
            self._stop_flag = True
            self.condition.notify_all()

    def flush(self):
        """Flush any pending data (no-op for base publisher)."""
        pass

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
        # Check if there's a frame ready to process
        if not self.frame_ready:
            return
        
        # Get the frame data
        data = self.frame_data
        self.frame_ready = False

        if len(data) == 4:
            frame, frame_index, trace_id, handoff_ts = data
            self.logger.debug(f"[PUB] processing idx={frame_index} trace_id={trace_id}")
        else:
            frame, frame_index, trace_id = data
            self.logger.debug(f"[PUB] processing idx={frame_index} trace_id={trace_id}")

        self.generate_and_send_event(frame, frame_index, trace_id)


    def generate_and_send_event(self, frame, frame_index, trace_id):
        # Get read timestamp for latency calculation
        read_ts = self._frame_read_timestamps.get(frame_index, time.perf_counter())
        
        # Capture t0 for this single frame
        frame_start = time.perf_counter()
        
        span_ctx = self.tracer.extract(Format.HTTP_HEADERS, {'uber-trace-id': trace_id})
        with self.tracer.start_active_span('generate_and_send_event', child_of=span_ctx) as scope:
            event_data = self.generate_event_from_frame(frame, frame_index)
            
            # ADD TRACE TAG
            scope.span.set_tag("frame_index", frame_index)
            scope.span.set_tag("batch_size", 1)  # For baseline comparison
            
            self.parent_service.write_event_with_trace(event_data, self.bufferstream)
            
            # Capture t1 for this single frame
            frame_end = time.perf_counter()
            total_time = frame_end - frame_start
            
            # Calculate throughput (for 1 frame)
            throughput = 1.0 / total_time
            
            # Calculate latency from read to sent
            latency_ms = (frame_end - read_ts) * 1000
            
            # ADD TRACE TAGS
            scope.span.set_tag("frame_total_time_s", total_time)
            scope.span.set_tag("frame_throughput_fps", throughput)
            scope.span.set_tag("frame_latency_ms", latency_ms)
            
            self._throughput_measurements.append((1, total_time))
            
            # Record metrics if collector available
            if self.metrics_collector:
                self.metrics_collector.record_frame_sent(
                    frame_index=frame_index,
                    sent_timestamp=frame_end,
                    batch_id=event_data["id"],
                    position_in_batch=0,
                    batch_size=1
                )
                self.metrics_collector.record_batch_sent(
                    batch_id=event_data["id"],
                    batch_size=1,
                    first_frame_index=frame_index,
                    last_frame_index=frame_index,
                    first_frame_read_ts=read_ts,
                    last_frame_sent_ts=frame_end,
                    triggered_by="size"
                )
            
            # Clean up read timestamp
            self._frame_read_timestamps.pop(frame_index, None)
            
            self.logger.info(
                f'📤 sending event_data "{event_data["id"][:40]}..." '
                f'| frame={frame_index} | time={total_time:.3f}s | latency={latency_ms:.2f}ms | throughput={throughput:.2f} FPS | '
                f'buffer_stream="{self.buffer_stream_key}"'
            )

    def generate_event_from_frame(self, frame, frame_index):
        # Get current UTC timestamp
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

        event_id = f'{self.publisher_details["publisher_id"]}-{str(uuid.uuid4())}'

        if REDUCE_SCALE > 1:
            frame = cv2.resize(frame, (self.width // REDUCE_SCALE, self.height // REDUCE_SCALE), interpolation=cv2.INTER_LINEAR)
            # workaround to the absurd ammount of mem usage in the server
            # by the cloudseg method, so try to make some space available as soon as
            # it can be made
            if frame_index < 1600:
                self.file_storage_cli.expiration_time = 240
            if frame_index < 328:
                self.file_storage_cli.expiration_time = 60

        img_uri = self.file_storage_cli.upload_inmemory_to_storage(frame)


        # store_size = getsizeof(frame.tobytes(order='C'))
        # store_size = self.file_storage_cli.client.execute_command(f'MEMORY USAGE {img_uri}')
        # self.store_sizes.append(int(store_size))
        # std = 0
        # if len(self.store_sizes) > 1:
        #     std = statistics.stdev(self.store_sizes)
        # self.parent_service.logger.error(f'>>> total ({len(self.store_sizes)}) last_size: {store_size} >> avg sizes: {sum(self.store_sizes) / len(self.store_sizes)} | std: {std}')
        event_data = {
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
        }
        return event_data
    
    def get_throughput_stats(self):
        """Calculate average throughput across all frames."""
        if not self._throughput_measurements:
            return {"avg_throughput_fps": 0, "total_frames": 0}
        
        total_frames = sum(frames for frames, _ in self._throughput_measurements)
        total_time = sum(time for _, time in self._throughput_measurements)
        
        avg_throughput = total_frames / total_time if total_time > 0 else 0
        
        return {
            "avg_throughput_fps": avg_throughput,
            "total_frames": total_frames,
            "total_time_s": total_time,
            "individual_frame_throughputs": [
                frames / time for frames, time in self._throughput_measurements
            ]
        }
    
    def save_throughput_stats(self, filepath=None):
        """Save throughput statistics to JSON file."""
        import json
        import os
        from adaptive_publisher.conf import PROJECT_ROOT
        
        if filepath is None:
            eval_dir = os.path.join(PROJECT_ROOT, 'data', 'eval')
            os.makedirs(eval_dir, exist_ok=True)
            filepath = os.path.join(eval_dir, 'baseline_throughput_stats.json')
        
        stats = self.get_throughput_stats()
        
        stats['config'] = {
            'publisher_id': self.publisher_details['publisher_id'],
            'source': self.publisher_details['source'],
            'resolution': self.publisher_details['meta']['resolution'],
            'fps': self.publisher_details['meta']['fps']
        }
        
        with open(filepath, 'w') as f:
            json.dump(stats, f, indent=4)
        
        self.logger.info(f'📊 Throughput stats saved to: {filepath}')
        self.logger.info(f'📊 Average throughput: {stats["avg_throughput_fps"]:.2f} FPS')
        
        return filepath
