import time
import threading
from collections import deque
from typing import Deque, List, Tuple, Optional
import json
import os
from opentracing.propagation import Format
from adaptive_publisher.event_publishers.publisher import EventPublisher
import datetime
import uuid
from adaptive_publisher.conf import PROJECT_ROOT


class MicroBatchingEventPublisher(EventPublisher):
    """
    Micro-batching event publisher that accumulates frames into batches
    before sending them to Redis.
    
    This reduces network overhead by grouping multiple frames into single
    transmission events, while tracking latency and throughput metrics.
    """

    def __init__(
        self,
        parent_service,
        publisher_details,
        query_ids,
        buffer_stream_key,
        batch_size: int = 10,
        batch_timeout: float = 0.1,
        metrics_collector=None,
    ):
        super().__init__(parent_service, publisher_details, query_ids, buffer_stream_key)

        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.metrics_collector = metrics_collector

        # Batch accumulation
        self._batch: List[Tuple] = []
        self._batch_start_time: Optional[float] = None
        self._first_frame_read_ts: Optional[float] = None
        
        # Throughput measurements
        self._throughput_measurements: List[Tuple[int, float]] = []
        
        # Per-frame timestamps for latency tracking
        self._frame_read_timestamps: dict = {}

    def record_frame_read(self, frame_index: int, read_timestamp: float):
        """Record when a frame was read (called from service)."""
        self._frame_read_timestamps[frame_index] = read_timestamp
        if self.metrics_collector:
            self.metrics_collector.record_frame_read(frame_index, read_timestamp)

    def generate_and_send_event(self, frame, frame_index, trace_id):
        """Accumulate frames and send when batch is full or timeout expires."""
        
        # Get read timestamp for this frame (or use current time if not recorded)
        read_ts = self._frame_read_timestamps.get(frame_index, time.perf_counter())
        
        # Start timing for this batch (first frame)
        if not self._batch:
            self._batch_start_time = time.perf_counter()
            self._first_frame_read_ts = read_ts
        
        # Add frame to batch with its read timestamp
        self._batch.append((frame, frame_index, trace_id, read_ts))
        
        # Check if batch should be sent
        should_send = False
        triggered_by = "size"
        
        if len(self._batch) >= self.batch_size:
            should_send = True
            triggered_by = "size"
        elif self._batch_start_time and (time.perf_counter() - self._batch_start_time) >= self.batch_timeout:
            should_send = True
            triggered_by = "timeout"
        
        if should_send:
            self._send_batch(triggered_by)

    def _send_batch(self, triggered_by: str = "size"):
        """Send accumulated batch with proper tracing."""
        if not self._batch:
            return
        
        items = self._batch
        self._batch = []
        
        batch_start = self._batch_start_time
        first_frame_read_ts = self._first_frame_read_ts
        
        first_frame_index = items[0][1]
        last_frame_index = items[-1][1]
        
        # Get trace context from first frame (for span correlation)
        first_trace_id = items[0][2]
        
        # Start tracing span for batch sending
        span_ctx = self.tracer.extract(Format.HTTP_HEADERS, {'uber-trace-id': first_trace_id})
        with self.tracer.start_active_span('generate_and_send_batch', child_of=span_ctx) as scope:
            # Add batch tracing tags
            scope.span.set_tag("batch_size", len(items))
            scope.span.set_tag("first_frame_index", first_frame_index)
            scope.span.set_tag("last_frame_index", last_frame_index)
            scope.span.set_tag("frame_indices", f"{first_frame_index}-{last_frame_index}")
            scope.span.set_tag("batch_frames", ','.join(str(item[1]) for item in items))
            scope.span.set_tag("triggered_by", triggered_by)
            
            # Generate event data for each frame (upload images to Redis)
            frames_data = []
            for i, (frame, frame_index, _trace, _read_ts) in enumerate(items):
                with self.tracer.start_active_span(f'generate_frame_{frame_index}', child_of=scope.span):
                    event_data = self.generate_event_from_frame(frame, frame_index)
                    frames_data.append({
                        "frame_index": event_data["frame_index"],
                        "image_url": event_data["image_url"],
                        "timestamp": event_data["timestamp"],
                    })
            
            # Create batched event
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
            
            # Send to stream (THE CRITICAL TRANSMISSION)
            with self.tracer.start_active_span('write_batch_to_stream', child_of=scope.span):
                self.parent_service.write_event_with_trace(batched_event_data, self.bufferstream)
            
            # Record send timestamp
            batch_end = time.perf_counter()
            
            # Calculate batch metrics
            total_time = batch_end - batch_start if batch_start else 0
            batch_throughput = len(items) / total_time if total_time > 0 else 0
            
            # Add metrics to span
            scope.span.set_tag("batch_total_time_s", total_time)
            scope.span.set_tag("batch_throughput_fps", batch_throughput)
            
            # Store throughput measurement
            self._throughput_measurements.append((len(items), total_time))
            
            # Record metrics for each frame
            if self.metrics_collector:
                for i, (frame, frame_index, _trace, read_ts) in enumerate(items):
                    self.metrics_collector.record_frame_sent(
                        frame_index=frame_index,
                        sent_timestamp=batch_end,
                        batch_id=event_id,
                        position_in_batch=i,
                        batch_size=len(items)
                    )
                
                # Record batch-level metrics
                self.metrics_collector.record_batch_sent(
                    batch_id=event_id,
                    batch_size=len(items),
                    first_frame_index=first_frame_index,
                    last_frame_index=last_frame_index,
                    first_frame_read_ts=first_frame_read_ts if first_frame_read_ts else batch_start,
                    last_frame_sent_ts=batch_end,
                    triggered_by=triggered_by
                )
            
            # Log batch transmission
            self.logger.info(
                f'📦 Sent batch ({len(items)} frames: {first_frame_index}-{last_frame_index}) '
                f'| triggered_by={triggered_by} | time={total_time:.3f}s | throughput={batch_throughput:.2f} FPS'
            )
        
        # Clear read timestamps for sent frames
        for _, frame_index, _, _ in items:
            self._frame_read_timestamps.pop(frame_index, None)
        
        # Reset for next batch
        self._batch_start_time = None
        self._first_frame_read_ts = None

    def flush(self):
        """Send any remaining frames (called at shutdown or timeout)."""
        if self._batch:
            self.logger.info(f'🔄 Flushing {len(self._batch)} remaining frames...')
            self._send_batch(triggered_by="flush")

    def get_throughput_stats(self):
        """Calculate average throughput statistics."""
        if not self._throughput_measurements:
            return {
                "avg_throughput_fps": 0,
                "total_batches": 0,
                "total_frames": 0,
                "total_time_s": 0,
                "batch_throughputs": []
            }
        
        total_frames = sum(frames for frames, _ in self._throughput_measurements)
        total_time = sum(t for _, t in self._throughput_measurements)
        avg_throughput = total_frames / total_time if total_time > 0 else 0
        
        batch_throughputs = [
            frames / t if t > 0 else 0 
            for frames, t in self._throughput_measurements
        ]
        
        return {
            "avg_throughput_fps": avg_throughput,
            "total_batches": len(self._throughput_measurements),
            "total_frames": total_frames,
            "total_time_s": total_time,
            "batch_throughputs": batch_throughputs
        }
    
    def save_throughput_stats(self, filepath=None):
        """Save stats to JSON."""
        if filepath is None:
            eval_dir = os.path.join(PROJECT_ROOT, 'data', 'eval')
            os.makedirs(eval_dir, exist_ok=True)
            filepath = os.path.join(eval_dir, 'microbatch_throughput_stats.json')
        
        stats = self.get_throughput_stats()
        stats['config'] = {
            'batch_size': self.batch_size,
            'batch_timeout': self.batch_timeout,
            'publisher_id': self.publisher_details['publisher_id'],
            'source': self.publisher_details['source'],
            'resolution': self.publisher_details['meta']['resolution'],
            'fps': self.publisher_details['meta']['fps']
        }
        
        with open(filepath, 'w') as f:
            json.dump(stats, f, indent=4)
        
        self.logger.info(f'📊 Stats saved: {filepath}')
        self.logger.info(f'📊 Avg throughput: {stats["avg_throughput_fps"]:.2f} FPS')
        
        return filepath