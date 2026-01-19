#!/usr/bin/env python3
"""
Scientific Analysis of Micro-Batching Optimization for Video Frame Publishing.

IMPORTANT: This analysis uses BATCH-LEVEL metrics as per Felipe's definition:
- Batch Latency (t1 - t0): Time from FIRST frame read to LAST frame sent
- Batch Throughput: batch_size / batch_latency

This unified formula works for both:
- Frame-by-frame (batch_size=1): latency = time to read and send 1 frame
- Micro-batching (batch_size=N): latency = time from first frame read to batch sent
"""

import json
import os
import glob
import statistics
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import numpy as np
import matplotlib.pyplot as plt

# Configure matplotlib for clean, professional style
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
    'font.size': 12,
    'axes.titlesize': 14,
    'axes.labelsize': 12,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 11,
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.1,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': '-',
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.axisbelow': True,
})

# Color scheme
COLORS = {
    'baseline': '#4472C4',
    'batch2': '#ED7D31',
    'batch3': '#A5A5A5',
    'batch4': '#FFC000',
    'batch5': '#5B9BD5',
    'no_comp': '#4472C4',
    'lz4': '#ED7D31',
    'qoi': '#A5A5A5',
}


@dataclass
class ExperimentResult:
    """
    Experiment results using BATCH-LEVEL metrics.
    
    Key metrics (per Felipe's definition):
    - batch_latency_ms: Time from first frame read (t0) to batch sent (t1)
    - batch_throughput_fps: batch_size / batch_latency
    """
    name: str
    display_name: str
    batch_size: int
    num_frames: int
    num_batches: int
    experiment_duration_s: float
    
    # Batch-level latency (t1 - t0) in milliseconds
    batch_latency_mean_ms: float
    batch_latency_std_ms: float
    batch_latency_p50_ms: float
    batch_latency_p90_ms: float
    batch_latency_p99_ms: float
    all_batch_latencies_ms: List[float]
    
    # Batch-level throughput (fps)
    batch_throughput_mean_fps: float
    batch_throughput_std_fps: float
    overall_throughput_fps: float
    all_batch_throughputs_fps: List[float]
    
    # Compression info
    compression: Optional[str] = None


def load_experiment_results(results_dir: str) -> Dict[str, ExperimentResult]:
    """Load experiment results, extracting BATCH-LEVEL metrics."""
    results = {}
    
    display_names = {
        'baseline': 'Baseline (n=1)',
        'batch2': 'Micro-batch (n=2)',
        'batch3': 'Micro-batch (n=3)',
        'batch4': 'Micro-batch (n=4)',
        'batch5': 'Micro-batch (n=5)',
        'batch2_lz4': 'n=2 + LZ4',
        'batch2_qoi': 'n=2 + QOI',
    }
    
    for json_file in glob.glob(os.path.join(results_dir, "experiment_*.json")):
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        filename = os.path.basename(json_file)
        parts = filename.replace('experiment_', '').split('_')
        name = parts[0]
        if len(parts) > 1 and parts[1] in ['lz4', 'qoi']:
            name = f"{parts[0]}_{parts[1]}"
        
        # Skip adaptive experiments
        if 'adaptative' in name:
            continue
        
        config = data['config']
        metadata = data['metadata']
        
        # Extract batch-level metrics
        batch_latencies_ms = []
        batch_throughputs_fps = []
        
        # Check for raw batch data
        if 'raw_data' in data and 'batch_metrics' in data['raw_data']:
            batches = data['raw_data']['batch_metrics']
            batch_latencies_ms = [b['total_time_s'] * 1000 for b in batches]  # Convert to ms
            batch_throughputs_fps = [b['throughput_fps'] for b in batches]
            num_batches = len(batches)
        elif 'throughput' in data:
            # Fallback to throughput stats if raw data not available
            throughput = data['throughput']
            batch_throughputs_fps = throughput.get('all_batch_throughputs_fps', [])
            num_batches = throughput.get('total_batches', 0)
            # Reconstruct batch latencies from throughput
            batch_size = config.get('batch_size', 1)
            batch_latencies_ms = [batch_size / t * 1000 if t > 0 else 0 for t in batch_throughputs_fps]
        else:
            # No batch data available, use frame-level as approximation
            latency = data['latency']
            batch_latencies_ms = latency.get('all_latencies_ms', [])
            num_batches = len(batch_latencies_ms)
            batch_throughputs_fps = [1000 / lat if lat > 0 else 0 for lat in batch_latencies_ms]
        
        # Calculate statistics
        if batch_latencies_ms:
            sorted_latencies = sorted(batch_latencies_ms)
            n = len(sorted_latencies)
            batch_latency_mean = statistics.mean(batch_latencies_ms)
            batch_latency_std = statistics.stdev(batch_latencies_ms) if n > 1 else 0
            batch_latency_p50 = sorted_latencies[int(n * 0.50)]
            batch_latency_p90 = sorted_latencies[min(int(n * 0.90), n-1)]
            batch_latency_p99 = sorted_latencies[min(int(n * 0.99), n-1)]
        else:
            batch_latency_mean = batch_latency_std = 0
            batch_latency_p50 = batch_latency_p90 = batch_latency_p99 = 0
        
        if batch_throughputs_fps:
            throughput_mean = statistics.mean(batch_throughputs_fps)
            throughput_std = statistics.stdev(batch_throughputs_fps) if len(batch_throughputs_fps) > 1 else 0
        else:
            throughput_mean = throughput_std = 0
        
        # Overall throughput
        if 'throughput' in data:
            overall_throughput = data['throughput'].get('overall_throughput_fps', 0)
        else:
            total_time = sum(batch_latencies_ms) / 1000 if batch_latencies_ms else 1
            overall_throughput = config.get('num_frames', 0) / total_time if total_time > 0 else 0
        
        # Compression type
        compression = None
        if 'lz4' in name:
            compression = 'LZ4'
        elif 'qoi' in name:
            compression = 'QOI'
        
        result = ExperimentResult(
            name=name,
            display_name=display_names.get(name, name),
            batch_size=config.get('batch_size', 1),
            num_frames=config.get('num_frames', 0),
            num_batches=num_batches,
            experiment_duration_s=metadata.get('experiment_duration_s', 0),
            batch_latency_mean_ms=batch_latency_mean,
            batch_latency_std_ms=batch_latency_std,
            batch_latency_p50_ms=batch_latency_p50,
            batch_latency_p90_ms=batch_latency_p90,
            batch_latency_p99_ms=batch_latency_p99,
            all_batch_latencies_ms=batch_latencies_ms,
            batch_throughput_mean_fps=throughput_mean,
            batch_throughput_std_fps=throughput_std,
            overall_throughput_fps=overall_throughput,
            all_batch_throughputs_fps=batch_throughputs_fps,
            compression=compression,
        )
        
        results[name] = result
    
    return results


# =============================================================================
# PLOTS
# =============================================================================

def plot_batch_latency(results: Dict[str, ExperimentResult], output_dir: str):
    """Bar chart: Batch latency (t1-t0) for each configuration."""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    experiments = ['baseline', 'batch2', 'batch3', 'batch4', 'batch5']
    data = [(results[n].display_name, results[n].batch_latency_mean_ms, 
             results[n].batch_latency_std_ms, COLORS[n]) 
            for n in experiments if n in results]
    
    if not data:
        return
    
    labels, means, stds, colors = zip(*data)
    x = np.arange(len(labels))
    
    bars = ax.bar(x, means, yerr=stds, capsize=5, color=colors, 
                  edgecolor='black', linewidth=0.8, alpha=0.9)
    
    ax.set_ylabel('Batch Latency (ms)')
    ax.set_xlabel('Configuration')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha='right')
    
    for bar, val in zip(bars, means):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, 
                f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'batch_latency.png'))
    plt.savefig(os.path.join(output_dir, 'batch_latency.pdf'))
    plt.close()
    print("✓ batch_latency")


def plot_batch_throughput(results: Dict[str, ExperimentResult], output_dir: str):
    """Bar chart: Batch throughput (fps) for each configuration."""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    experiments = ['baseline', 'batch2', 'batch3', 'batch4', 'batch5']
    data = [(results[n].display_name, results[n].overall_throughput_fps, COLORS[n]) 
            for n in experiments if n in results]
    
    if not data:
        return
    
    labels, throughputs, colors = zip(*data)
    x = np.arange(len(labels))
    
    bars = ax.bar(x, throughputs, color=colors, edgecolor='black', linewidth=0.8, alpha=0.9)
    
    ax.set_ylabel('Throughput (fps)')
    ax.set_xlabel('Configuration')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha='right')
    
    # Target line at 30 fps
    ax.axhline(y=30, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Target (30 fps)')
    ax.legend(loc='upper right')
    
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, 
                f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'batch_throughput.png'))
    plt.savefig(os.path.join(output_dir, 'batch_throughput.pdf'))
    plt.close()
    print("✓ batch_throughput")


def plot_latency_vs_batchsize(results: Dict[str, ExperimentResult], output_dir: str):
    """Line chart: Batch latency vs batch size with error bars."""
    fig, ax = plt.subplots(figsize=(7, 5))
    
    experiments = ['baseline', 'batch2', 'batch3', 'batch4', 'batch5']
    exp_data = [results[n] for n in experiments if n in results]
    
    if not exp_data:
        return
    
    batch_sizes = [e.batch_size for e in exp_data]
    means = [e.batch_latency_mean_ms for e in exp_data]
    stds = [e.batch_latency_std_ms for e in exp_data]
    
    ax.errorbar(batch_sizes, means, yerr=stds, fmt='o-', capsize=5, capthick=2,
                markersize=10, linewidth=2, color='#4472C4', ecolor='gray')
    
    ax.set_ylabel('Batch Latency (ms)')
    ax.set_xlabel('Batch Size (n)')
    ax.set_xticks(batch_sizes)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'latency_vs_batchsize.png'))
    plt.savefig(os.path.join(output_dir, 'latency_vs_batchsize.pdf'))
    plt.close()
    print("✓ latency_vs_batchsize")


def plot_throughput_vs_batchsize(results: Dict[str, ExperimentResult], output_dir: str):
    """Line chart: Throughput vs batch size."""
    fig, ax = plt.subplots(figsize=(7, 5))
    
    experiments = ['baseline', 'batch2', 'batch3', 'batch4', 'batch5']
    exp_data = [results[n] for n in experiments if n in results]
    
    if not exp_data:
        return
    
    batch_sizes = [e.batch_size for e in exp_data]
    throughputs = [e.overall_throughput_fps for e in exp_data]
    
    ax.plot(batch_sizes, throughputs, 'o-', markersize=10, linewidth=2, color='#4472C4')
    
    # Target line
    ax.axhline(y=30, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Target (30 fps)')
    
    ax.set_ylabel('Throughput (fps)')
    ax.set_xlabel('Batch Size (n)')
    ax.set_xticks(batch_sizes)
    ax.legend(loc='lower right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'throughput_vs_batchsize.png'))
    plt.savefig(os.path.join(output_dir, 'throughput_vs_batchsize.pdf'))
    plt.close()
    print("✓ throughput_vs_batchsize")


def plot_compression_latency(results: Dict[str, ExperimentResult], output_dir: str):
    """Bar chart: Batch latency comparing compression methods."""
    fig, ax = plt.subplots(figsize=(7, 5))
    
    experiments = ['batch2', 'batch2_lz4', 'batch2_qoi']
    exp_labels = ['No Compression', 'LZ4', 'QOI']
    colors = [COLORS['no_comp'], COLORS['lz4'], COLORS['qoi']]
    
    data = []
    for n, label, color in zip(experiments, exp_labels, colors):
        if n in results:
            data.append((label, results[n].batch_latency_mean_ms, 
                        results[n].batch_latency_std_ms, color))
    
    if not data:
        return
    
    labels, means, stds, colors = zip(*data)
    x = np.arange(len(labels))
    
    bars = ax.bar(x, means, yerr=stds, capsize=5, color=colors,
                  edgecolor='black', linewidth=0.8, alpha=0.9)
    
    ax.set_ylabel('Batch Latency (ms)')
    ax.set_xlabel('Compression Method')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    
    for bar, val in zip(bars, means):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'compression_latency.png'))
    plt.savefig(os.path.join(output_dir, 'compression_latency.pdf'))
    plt.close()
    print("✓ compression_latency")


def plot_compression_throughput(results: Dict[str, ExperimentResult], output_dir: str):
    """Bar chart: Throughput comparing compression methods."""
    fig, ax = plt.subplots(figsize=(7, 5))
    
    experiments = ['batch2', 'batch2_lz4', 'batch2_qoi']
    exp_labels = ['No Compression', 'LZ4', 'QOI']
    colors = [COLORS['no_comp'], COLORS['lz4'], COLORS['qoi']]
    
    data = []
    for n, label, color in zip(experiments, exp_labels, colors):
        if n in results:
            data.append((label, results[n].overall_throughput_fps, color))
    
    if not data:
        return
    
    labels, throughputs, colors = zip(*data)
    x = np.arange(len(labels))
    
    bars = ax.bar(x, throughputs, color=colors, edgecolor='black', linewidth=0.8, alpha=0.9)
    
    ax.set_ylabel('Throughput (fps)')
    ax.set_xlabel('Compression Method')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'compression_throughput.png'))
    plt.savefig(os.path.join(output_dir, 'compression_throughput.pdf'))
    plt.close()
    print("✓ compression_throughput")


def generate_latex_table(results: Dict[str, ExperimentResult], output_dir: str):
    """Generate LaTeX table with batch-level metrics."""
    
    experiment_order = [
        'baseline', 'batch2', 'batch3', 'batch4', 'batch5',
        'batch2_lz4', 'batch2_qoi'
    ]
    
    latex = r"""\begin{table}[htbp]
\centering
\caption{Micro-Batching Performance (Batch-Level Metrics)}
\label{tab:microbatch}
\begin{tabular}{lccccc}
\toprule
\textbf{Configuration} & \textbf{Batch Size} & \textbf{Latency (ms)} & \textbf{Std (ms)} & \textbf{P90 (ms)} & \textbf{Throughput (fps)} \\
\midrule
"""
    
    for name in experiment_order:
        if name in results:
            e = results[name]
            latex += f"{e.display_name} & {e.batch_size} & {e.batch_latency_mean_ms:.2f} & "
            latex += f"{e.batch_latency_std_ms:.2f} & {e.batch_latency_p90_ms:.2f} & "
            latex += f"{e.overall_throughput_fps:.1f} \\\\\n"
    
    latex += r"""\bottomrule
\end{tabular}
\smallskip
\small{Note: Latency = time from first frame read (t0) to batch sent (t1). Throughput = batch\_size / latency.}
\end{table}
"""
    
    with open(os.path.join(output_dir, 'microbatch_table.tex'), 'w') as f:
        f.write(latex)
    
    print("✓ microbatch_table.tex")


def print_summary(results: Dict[str, ExperimentResult]):
    """Print analysis summary with batch-level metrics."""
    print("\n" + "="*70)
    print("MICRO-BATCHING ANALYSIS (BATCH-LEVEL METRICS)")
    print("="*70)
    print("\nMetric definitions (per Felipe's email):")
    print("  - Batch Latency: t1 - t0 (first frame read to batch sent)")
    print("  - Throughput: batch_size / batch_latency")
    print()
    
    print("1. BATCH SIZE COMPARISON")
    print("-" * 50)
    print(f"{'Config':<20} {'Batch Size':>10} {'Latency (ms)':>15} {'Throughput (fps)':>18}")
    print("-" * 50)
    for name in ['baseline', 'batch2', 'batch3', 'batch4', 'batch5']:
        if name in results:
            e = results[name]
            print(f"{e.display_name:<20} {e.batch_size:>10} {e.batch_latency_mean_ms:>15.1f} {e.overall_throughput_fps:>18.1f}")
    
    print("\n2. COMPRESSION IMPACT (n=2)")
    print("-" * 50)
    base = results.get('batch2')
    if base:
        print(f"{'Config':<20} {'Latency (ms)':>15} {'Δ Latency':>12} {'Throughput':>12}")
        print("-" * 50)
        print(f"{'No Compression':<20} {base.batch_latency_mean_ms:>15.1f} {'-':>12} {base.overall_throughput_fps:>12.1f}")
        for comp in ['batch2_lz4', 'batch2_qoi']:
            if comp in results:
                e = results[comp]
                delta = ((e.batch_latency_mean_ms - base.batch_latency_mean_ms) / base.batch_latency_mean_ms) * 100
                print(f"{e.compression:<20} {e.batch_latency_mean_ms:>15.1f} {delta:>+11.1f}% {e.overall_throughput_fps:>12.1f}")
    
    print("\n" + "="*70 + "\n")


def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    results_dir = project_root / "data" / "experiment_results"
    output_dir = results_dir / "plots"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean old plots
    for f in output_dir.glob("*.png"):
        f.unlink()
    for f in output_dir.glob("*.pdf"):
        f.unlink()
    for f in output_dir.glob("*.tex"):
        f.unlink()
    
    print("\n" + "="*50)
    print("MICRO-BATCHING ANALYSIS (Batch-Level Metrics)")
    print("="*50 + "\n")
    
    results = load_experiment_results(str(results_dir))
    print(f"Loaded {len(results)} experiments\n")
    
    print("Generating plots...\n")
    
    # Micro-batching plots
    plot_batch_latency(results, str(output_dir))
    plot_batch_throughput(results, str(output_dir))
    plot_latency_vs_batchsize(results, str(output_dir))
    plot_throughput_vs_batchsize(results, str(output_dir))
    
    # Compression plots
    plot_compression_latency(results, str(output_dir))
    plot_compression_throughput(results, str(output_dir))
    
    # LaTeX table
    generate_latex_table(results, str(output_dir))
    
    print_summary(results)
    
    print(f"All plots saved to: {output_dir}\n")


if __name__ == "__main__":
    main()
