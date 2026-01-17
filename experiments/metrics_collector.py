"""
Metrics Collector for Micro-Batching Experiments

Collects latency and throughput metrics for analysis.
"""

import time
import json
import os
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict

from adaptive_publisher.conf import PROJECT_ROOT


@dataclass
class FrameMetrics:
    """Metrics for a single frame."""
    frame_index: int
    read_timestamp: float
    sent_timestamp: float = 0.0
    batch_id: Optional[str] = None
    position_in_batch: int = 0
    batch_size: int = 1
    
    @property
    def latency_ms(self) -> float:
        """End-to-end latency in milliseconds."""
        return (self.sent_timestamp - self.read_timestamp) * 1000
    
    @property
    def batch_wait_time_ms(self) -> float:
        """Time waiting for batch to fill (for non-first frames)."""
        # Approximate: position_in_batch * frame_interval
        frame_interval_ms = 33.33  # 30 FPS
        return self.position_in_batch * frame_interval_ms


@dataclass
class BatchMetrics:
    """Metrics for a single batch."""
    batch_id: str
    batch_size: int
    first_frame_index: int
    last_frame_index: int
    first_frame_read_ts: float
    last_frame_sent_ts: float
    triggered_by: str = "size"  # "size" or "timeout"
    
    @property
    def total_time_s(self) -> float:
        """Total time from first frame read to batch sent."""
        return self.last_frame_sent_ts - self.first_frame_read_ts
    
    @property
    def throughput_fps(self) -> float:
        """Throughput in frames per second."""
        if self.total_time_s > 0:
            return self.batch_size / self.total_time_s
        return 0.0


@dataclass
class ExperimentConfig:
    """Configuration for an experiment run."""
    batch_size: int
    batch_timeout: float
    num_frames: int
    fps: float
    resolution: str
    publisher_id: str
    source: str
    use_micro_batching: bool
    
    def to_dict(self) -> dict:
        return asdict(self)


class MetricsCollector:
    """Collects and aggregates metrics during experiments."""
    
    def __init__(self, config: ExperimentConfig):
        self.config = config
        self.frame_metrics: List[FrameMetrics] = []
        self.batch_metrics: List[BatchMetrics] = []
        self._pending_frames: Dict[int, FrameMetrics] = {}
        self._experiment_start_time: Optional[float] = None
        self._experiment_end_time: Optional[float] = None
    
    def start_experiment(self):
        """Mark experiment start."""
        self._experiment_start_time = time.perf_counter()
    
    def end_experiment(self):
        """Mark experiment end."""
        self._experiment_end_time = time.perf_counter()
    
    def record_frame_read(self, frame_index: int, read_timestamp: float):
        """Record when a frame was read."""
        metrics = FrameMetrics(
            frame_index=frame_index,
            read_timestamp=read_timestamp
        )
        self._pending_frames[frame_index] = metrics
    
    def record_frame_sent(
        self, 
        frame_index: int, 
        sent_timestamp: float,
        batch_id: Optional[str] = None,
        position_in_batch: int = 0,
        batch_size: int = 1
    ):
        """Record when a frame was sent."""
        if frame_index in self._pending_frames:
            metrics = self._pending_frames.pop(frame_index)
            metrics.sent_timestamp = sent_timestamp
            metrics.batch_id = batch_id
            metrics.position_in_batch = position_in_batch
            metrics.batch_size = batch_size
            self.frame_metrics.append(metrics)
    
    def record_batch_sent(
        self,
        batch_id: str,
        batch_size: int,
        first_frame_index: int,
        last_frame_index: int,
        first_frame_read_ts: float,
        last_frame_sent_ts: float,
        triggered_by: str = "size"
    ):
        """Record batch-level metrics."""
        batch = BatchMetrics(
            batch_id=batch_id,
            batch_size=batch_size,
            first_frame_index=first_frame_index,
            last_frame_index=last_frame_index,
            first_frame_read_ts=first_frame_read_ts,
            last_frame_sent_ts=last_frame_sent_ts,
            triggered_by=triggered_by
        )
        self.batch_metrics.append(batch)
    
    def get_latency_stats(self) -> Dict:
        """Calculate latency statistics."""
        if not self.frame_metrics:
            return self._empty_latency_stats()
        
        latencies = [f.latency_ms for f in self.frame_metrics]
        
        sorted_latencies = sorted(latencies)
        n = len(sorted_latencies)
        
        def percentile(p):
            idx = int(n * p / 100)
            return sorted_latencies[min(idx, n - 1)]
        
        return {
            "count": n,
            "mean_ms": statistics.mean(latencies),
            "median_ms": statistics.median(latencies),
            "std_ms": statistics.stdev(latencies) if n > 1 else 0,
            "min_ms": min(latencies),
            "max_ms": max(latencies),
            "p50_ms": percentile(50),
            "p75_ms": percentile(75),
            "p90_ms": percentile(90),
            "p95_ms": percentile(95),
            "p99_ms": percentile(99),
            "all_latencies_ms": latencies
        }
    
    def _empty_latency_stats(self) -> Dict:
        return {
            "count": 0,
            "mean_ms": 0,
            "median_ms": 0,
            "std_ms": 0,
            "min_ms": 0,
            "max_ms": 0,
            "p50_ms": 0,
            "p75_ms": 0,
            "p90_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "all_latencies_ms": []
        }
    
    def get_throughput_stats(self) -> Dict:
        """Calculate throughput statistics."""
        if not self.batch_metrics:
            return self._empty_throughput_stats()
        
        throughputs = [b.throughput_fps for b in self.batch_metrics]
        total_frames = sum(b.batch_size for b in self.batch_metrics)
        total_time = sum(b.total_time_s for b in self.batch_metrics)
        
        # Overall throughput
        overall_throughput = total_frames / total_time if total_time > 0 else 0
        
        # Per-batch throughput stats
        sorted_throughputs = sorted(throughputs)
        n = len(sorted_throughputs)
        
        def percentile(p):
            idx = int(n * p / 100)
            return sorted_throughputs[min(idx, n - 1)]
        
        return {
            "total_frames": total_frames,
            "total_batches": n,
            "total_time_s": total_time,
            "overall_throughput_fps": overall_throughput,
            "mean_batch_throughput_fps": statistics.mean(throughputs),
            "median_batch_throughput_fps": statistics.median(throughputs),
            "std_batch_throughput_fps": statistics.stdev(throughputs) if n > 1 else 0,
            "min_batch_throughput_fps": min(throughputs),
            "max_batch_throughput_fps": max(throughputs),
            "p50_batch_throughput_fps": percentile(50),
            "p90_batch_throughput_fps": percentile(90),
            "all_batch_throughputs_fps": throughputs,
            "target_fps": self.config.fps,
            "throughput_ratio": overall_throughput / self.config.fps if self.config.fps > 0 else 0
        }
    
    def _empty_throughput_stats(self) -> Dict:
        return {
            "total_frames": 0,
            "total_batches": 0,
            "total_time_s": 0,
            "overall_throughput_fps": 0,
            "mean_batch_throughput_fps": 0,
            "median_batch_throughput_fps": 0,
            "std_batch_throughput_fps": 0,
            "min_batch_throughput_fps": 0,
            "max_batch_throughput_fps": 0,
            "p50_batch_throughput_fps": 0,
            "p90_batch_throughput_fps": 0,
            "all_batch_throughputs_fps": [],
            "target_fps": self.config.fps,
            "throughput_ratio": 0
        }
    
    def get_network_efficiency_stats(self) -> Dict:
        """Calculate network efficiency metrics."""
        total_frames = len(self.frame_metrics)
        total_events = len(self.batch_metrics)
        
        # Baseline comparison (1 event per frame)
        baseline_events = total_frames
        
        if baseline_events > 0:
            reduction_percent = (1 - total_events / baseline_events) * 100
        else:
            reduction_percent = 0
        
        # Events per second
        if self._experiment_end_time and self._experiment_start_time:
            experiment_duration = self._experiment_end_time - self._experiment_start_time
            events_per_second = total_events / experiment_duration if experiment_duration > 0 else 0
        else:
            events_per_second = 0
        
        # Batch efficiency
        triggered_by_size = sum(1 for b in self.batch_metrics if b.triggered_by == "size")
        triggered_by_timeout = sum(1 for b in self.batch_metrics if b.triggered_by == "timeout")
        
        avg_batch_size = statistics.mean([b.batch_size for b in self.batch_metrics]) if self.batch_metrics else 0
        
        return {
            "total_frames": total_frames,
            "total_events_sent": total_events,
            "baseline_events": baseline_events,
            "network_reduction_percent": reduction_percent,
            "events_per_second": events_per_second,
            "avg_batch_size_achieved": avg_batch_size,
            "configured_batch_size": self.config.batch_size,
            "batch_fill_ratio": avg_batch_size / self.config.batch_size if self.config.batch_size > 0 else 0,
            "batches_triggered_by_size": triggered_by_size,
            "batches_triggered_by_timeout": triggered_by_timeout
        }
    
    def get_full_report(self) -> Dict:
        """Generate complete experiment report."""
        return {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "experiment_duration_s": (
                    self._experiment_end_time - self._experiment_start_time
                    if self._experiment_end_time and self._experiment_start_time
                    else 0
                )
            },
            "config": self.config.to_dict(),
            "latency": self.get_latency_stats(),
            "throughput": self.get_throughput_stats(),
            "network_efficiency": self.get_network_efficiency_stats(),
            "raw_data": {
                "frame_metrics": [
                    {
                        "frame_index": f.frame_index,
                        "latency_ms": f.latency_ms,
                        "batch_id": f.batch_id,
                        "position_in_batch": f.position_in_batch,
                        "batch_size": f.batch_size
                    }
                    for f in self.frame_metrics
                ],
                "batch_metrics": [
                    {
                        "batch_id": b.batch_id,
                        "batch_size": b.batch_size,
                        "first_frame_index": b.first_frame_index,
                        "last_frame_index": b.last_frame_index,
                        "total_time_s": b.total_time_s,
                        "throughput_fps": b.throughput_fps,
                        "triggered_by": b.triggered_by
                    }
                    for b in self.batch_metrics
                ]
            }
        }
    
    def save_report(self, filepath: Optional[str] = None) -> str:
        """Save experiment report to JSON file."""
        if filepath is None:
            results_dir = os.path.join(PROJECT_ROOT, 'data', 'experiment_results')
            os.makedirs(results_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_label = f"batch{self.config.batch_size}" if self.config.use_micro_batching else "baseline"
            filepath = os.path.join(
                results_dir, 
                f'experiment_{batch_label}_{timestamp}.json'
            )
        
        report = self.get_full_report()
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)
        
        return filepath


def create_comparison_report(experiment_results: List[Dict], output_path: Optional[str] = None) -> Dict:
    """
    Create a comparison report across multiple experiment configurations.
    
    Args:
        experiment_results: List of experiment reports from different configurations
        output_path: Optional path to save the comparison report
    
    Returns:
        Comparison report dictionary
    """
    comparison = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "num_experiments": len(experiment_results)
        },
        "configurations": [],
        "comparison_table": {
            "throughput": [],
            "latency": [],
            "network_efficiency": []
        },
        "analysis": {}
    }
    
    for result in experiment_results:
        config = result.get("config", {})
        batch_size = config.get("batch_size", 1)
        
        # Configuration summary
        comparison["configurations"].append({
            "batch_size": batch_size,
            "batch_timeout": config.get("batch_timeout", 0),
            "use_micro_batching": config.get("use_micro_batching", False)
        })
        
        # Throughput comparison
        throughput = result.get("throughput", {})
        comparison["comparison_table"]["throughput"].append({
            "batch_size": batch_size,
            "overall_fps": throughput.get("overall_throughput_fps", 0),
            "mean_batch_fps": throughput.get("mean_batch_throughput_fps", 0),
            "target_fps": throughput.get("target_fps", 30),
            "throughput_ratio": throughput.get("throughput_ratio", 0)
        })
        
        # Latency comparison
        latency = result.get("latency", {})
        comparison["comparison_table"]["latency"].append({
            "batch_size": batch_size,
            "mean_ms": latency.get("mean_ms", 0),
            "median_ms": latency.get("median_ms", 0),
            "p90_ms": latency.get("p90_ms", 0),
            "p99_ms": latency.get("p99_ms", 0),
            "std_ms": latency.get("std_ms", 0)
        })
        
        # Network efficiency comparison
        network = result.get("network_efficiency", {})
        comparison["comparison_table"]["network_efficiency"].append({
            "batch_size": batch_size,
            "events_sent": network.get("total_events_sent", 0),
            "reduction_percent": network.get("network_reduction_percent", 0),
            "events_per_second": network.get("events_per_second", 0),
            "avg_batch_size_achieved": network.get("avg_batch_size_achieved", 0)
        })
    
    # Analysis: Find optimal configuration
    if experiment_results:
        # Score each configuration (throughput_ratio - normalized_latency_increase)
        baseline_latency = None
        for item in comparison["comparison_table"]["latency"]:
            if item["batch_size"] == 1:
                baseline_latency = item["mean_ms"]
                break
        
        if baseline_latency is None and comparison["comparison_table"]["latency"]:
            baseline_latency = min(item["mean_ms"] for item in comparison["comparison_table"]["latency"])
        
        scores = []
        for i, result in enumerate(experiment_results):
            batch_size = comparison["configurations"][i]["batch_size"]
            throughput_ratio = comparison["comparison_table"]["throughput"][i]["throughput_ratio"]
            mean_latency = comparison["comparison_table"]["latency"][i]["mean_ms"]
            network_reduction = comparison["comparison_table"]["network_efficiency"][i]["reduction_percent"]
            
            # Normalize latency (lower is better)
            latency_penalty = (mean_latency / baseline_latency - 1) if baseline_latency and baseline_latency > 0 else 0
            
            # Score formula: prioritize throughput, penalize latency increase, reward network efficiency
            score = (
                0.4 * throughput_ratio +
                0.3 * (1 - min(latency_penalty, 1)) +
                0.3 * (network_reduction / 100)
            )
            
            scores.append({
                "batch_size": batch_size,
                "score": score,
                "throughput_ratio": throughput_ratio,
                "latency_penalty": latency_penalty,
                "network_reduction": network_reduction
            })
        
        # Sort by score
        scores.sort(key=lambda x: x["score"], reverse=True)
        comparison["analysis"]["ranking"] = scores
        comparison["analysis"]["optimal_config"] = scores[0] if scores else None
        comparison["analysis"]["baseline_latency_ms"] = baseline_latency
    
    # Save if path provided
    if output_path:
        with open(output_path, 'w') as f:
            json.dump(comparison, f, indent=2)
    
    return comparison
