"""
Adaptive Micro-Batching Event Publisher

Dynamically adjusts batch size based on real-time system feedback:
- Network performance (batch send times)
- Queue pressure (pending frames)

The algorithm is simple:
- If batches send quickly → increase batch size (better efficiency)
- If batches send slowly → decrease batch size (lower latency)
"""

import time
from typing import List, Optional
from collections import deque

from adaptive_publisher.event_publishers.batched_publisher import MicroBatchingEventPublisher


class AdaptiveBatchingEventPublisher(MicroBatchingEventPublisher):
    """
    Adaptive micro-batching publisher that adjusts batch size dynamically
    based on observed network conditions and processing times.
    """

    def __init__(
        self,
        parent_service,
        publisher_details,
        query_ids,
        buffer_stream_key,
        # Adaptive parameters
        min_batch_size: int = 1,
        max_batch_size: int = 10,
        initial_batch_size: int = 3,
        batch_timeout: float = 0.5,
        # Adaptation thresholds
        target_batch_time_ms: float = 150.0,  # Target time to send a batch
        adaptation_window: int = 5,  # Number of batches to average
        metrics_collector=None,
    ):
        # Initialize with initial batch size
        super().__init__(
            parent_service=parent_service,
            publisher_details=publisher_details,
            query_ids=query_ids,
            buffer_stream_key=buffer_stream_key,
            batch_size=initial_batch_size,
            batch_timeout=batch_timeout,
            metrics_collector=metrics_collector,
        )

        # Adaptive batching parameters
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.target_batch_time_ms = target_batch_time_ms
        self.adaptation_window = adaptation_window

        # History for adaptation decisions
        self._batch_send_times: deque = deque(maxlen=adaptation_window)
        self._batch_sizes_history: deque = deque(maxlen=adaptation_window)
        
        # Current adaptive state
        self._current_batch_size = initial_batch_size
        self._total_adaptations = 0
        self._adaptations_up = 0
        self._adaptations_down = 0

        self.logger.info(
            f'📊 Adaptive batching initialized: '
            f'min={min_batch_size}, max={max_batch_size}, initial={initial_batch_size}, '
            f'target_time={target_batch_time_ms}ms'
        )

    def _adapt_batch_size(self, last_batch_send_time_ms: float, last_batch_size: int):
        """
        Adapt batch size based on recent performance.
        
        Simple control algorithm:
        - If avg batch send time < target: increase batch size (network has capacity)
        - If avg batch send time > target: decrease batch size (reduce latency)
        """
        # Record this batch's metrics
        self._batch_send_times.append(last_batch_send_time_ms)
        self._batch_sizes_history.append(last_batch_size)

        # Need enough history to make decisions
        if len(self._batch_send_times) < 2:
            return

        # Calculate average batch send time (total time to send a batch)
        avg_send_time = sum(self._batch_send_times) / len(self._batch_send_times)

        old_batch_size = self._current_batch_size

        # Adaptation logic based on TOTAL batch send time
        if avg_send_time < self.target_batch_time_ms * 0.9:
            # Batches sending fast - we have bandwidth headroom, increase batch size
            self._current_batch_size = min(self._current_batch_size + 1, self.max_batch_size)
            if self._current_batch_size > old_batch_size:
                self._adaptations_up += 1
                self._total_adaptations += 1
                self.logger.info(
                    f'🔼 Batch size INCREASED: {old_batch_size} → {self._current_batch_size} '
                    f'(avg_send_time={avg_send_time:.1f}ms < target*0.7={self.target_batch_time_ms * 0.7:.1f}ms)'
                )
                
        elif avg_send_time > self.target_batch_time_ms * 1.1:
            # Batches sending slow - reduce batch size to lower latency
            self._current_batch_size = max(self._current_batch_size - 1, self.min_batch_size)
            if self._current_batch_size < old_batch_size:
                self._adaptations_down += 1
                self._total_adaptations += 1
                self.logger.info(
                    f'� Batch size DECREASED: {old_batch_size} → {self._current_batch_size} '
                    f'(avg_send_time={avg_send_time:.1f}ms > target*1.3={self.target_batch_time_ms * 1.3:.1f}ms)'
                )

        # Update the effective batch size
        self.batch_size = self._current_batch_size

    def _send_batch(self, triggered_by: str = "size"):
        """Override to track send times and adapt batch size."""
        if not self._batch:
            return

        batch_size = len(self._batch)

        # Call parent's send logic (this sets _last_batch_total_time_ms)
        super()._send_batch(triggered_by)

        # Get the REAL batch time from parent (includes all processing + network time)
        real_batch_time_ms = self._last_batch_total_time_ms

        # DEBUG: Log actual measured time
        self.logger.info(
            f'⏱️ Batch sent: size={batch_size}, REAL_time={real_batch_time_ms:.1f}ms, '
            f'target={self.target_batch_time_ms}ms'
        )

        # Adapt based on REAL performance
        self._adapt_batch_size(real_batch_time_ms, batch_size)

    def get_adaptive_stats(self) -> dict:
        """Get statistics about adaptive behavior."""
        return {
            "current_batch_size": self._current_batch_size,
            "min_batch_size": self.min_batch_size,
            "max_batch_size": self.max_batch_size,
            "target_batch_time_ms": self.target_batch_time_ms,
            "total_adaptations": self._total_adaptations,
            "adaptations_up": self._adaptations_up,
            "adaptations_down": self._adaptations_down,
            "recent_send_times_ms": list(self._batch_send_times),
            "recent_batch_sizes": list(self._batch_sizes_history),
            "avg_recent_send_time_ms": (
                sum(self._batch_send_times) / len(self._batch_send_times)
                if self._batch_send_times else 0
            ),
        }

    def flush(self):
        """Flush and log adaptive stats."""
        super().flush()
        
        stats = self.get_adaptive_stats()
        self.logger.info('='*50)
        self.logger.info('📊 ADAPTIVE BATCHING SUMMARY')
        self.logger.info('='*50)
        self.logger.info(f'  Final batch size: {stats["current_batch_size"]}')
        self.logger.info(f'  Total adaptations: {stats["total_adaptations"]}')
        self.logger.info(f'  Adaptations up: {stats["adaptations_up"]}')
        self.logger.info(f'  Adaptations down: {stats["adaptations_down"]}')
        self.logger.info(f'  Avg send time: {stats["avg_recent_send_time_ms"]:.1f}ms')
        self.logger.info('='*50)
