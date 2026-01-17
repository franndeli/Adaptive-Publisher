#!/usr/bin/env python
"""
Micro-Batching Experiment Analysis

Analyzes experiment results, generates comparison tables,
creates visualizations, and determines optimal configuration.

Usage:
    python -m experiments.analyze_results --results-dir data/experiment_results/
"""

import argparse
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional
import statistics

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adaptive_publisher.conf import PROJECT_ROOT

# Try to import matplotlib for visualization
try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("⚠️ matplotlib not available. Visualizations will be skipped.")


class ExperimentAnalyzer:
    """Analyzes micro-batching experiment results."""
    
    def __init__(self, results_dir: str):
        self.results_dir = results_dir
        self.experiment_results: List[Dict] = []
        self.comparison_report: Optional[Dict] = None
    
    def load_results(self, pattern: str = "experiment_batch*.json") -> int:
        """Load experiment result files."""
        import glob
        
        files = glob.glob(os.path.join(self.results_dir, pattern))
        
        # Also try the newer naming pattern
        files += glob.glob(os.path.join(self.results_dir, "experiment_*.json"))
        
        # Deduplicate
        files = list(set(files))
        
        # Filter out plan files and comparison files
        files = [f for f in files if 'plan' not in f and 'comparison' not in f]
        
        print(f"📂 Found {len(files)} result files")
        
        for filepath in sorted(files):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    data['_source_file'] = filepath
                    self.experiment_results.append(data)
                    batch_size = data.get('config', {}).get('batch_size', 'unknown')
                    print(f"   ✓ Loaded: {os.path.basename(filepath)} (batch_size={batch_size})")
            except Exception as e:
                print(f"   ✗ Error loading {filepath}: {e}")
        
        # Sort by batch size
        self.experiment_results.sort(
            key=lambda x: x.get('config', {}).get('batch_size', 0)
        )
        
        return len(self.experiment_results)
    
    def generate_comparison_tables(self) -> Dict:
        """Generate comparison tables across all experiments."""
        if not self.experiment_results:
            return {}
        
        tables = {
            "throughput": [],
            "latency": [],
            "network_efficiency": []
        }
        
        for result in self.experiment_results:
            config = result.get('config', {})
            batch_size = config.get('batch_size', 1)
            
            # Throughput table
            throughput = result.get('throughput', {})
            tables["throughput"].append({
                "batch_size": batch_size,
                "overall_fps": round(throughput.get('overall_throughput_fps', 0), 2),
                "mean_batch_fps": round(throughput.get('mean_batch_throughput_fps', 0), 2),
                "std_fps": round(throughput.get('std_batch_throughput_fps', 0), 2),
                "target_fps": throughput.get('target_fps', 30),
                "throughput_ratio": round(throughput.get('throughput_ratio', 0) * 100, 1),
                "total_frames": throughput.get('total_frames', 0),
                "total_batches": throughput.get('total_batches', 0)
            })
            
            # Latency table
            latency = result.get('latency', {})
            tables["latency"].append({
                "batch_size": batch_size,
                "mean_ms": round(latency.get('mean_ms', 0), 2),
                "median_ms": round(latency.get('median_ms', 0), 2),
                "std_ms": round(latency.get('std_ms', 0), 2),
                "min_ms": round(latency.get('min_ms', 0), 2),
                "max_ms": round(latency.get('max_ms', 0), 2),
                "p50_ms": round(latency.get('p50_ms', 0), 2),
                "p75_ms": round(latency.get('p75_ms', 0), 2),
                "p90_ms": round(latency.get('p90_ms', 0), 2),
                "p95_ms": round(latency.get('p95_ms', 0), 2),
                "p99_ms": round(latency.get('p99_ms', 0), 2)
            })
            
            # Network efficiency table
            network = result.get('network_efficiency', {})
            tables["network_efficiency"].append({
                "batch_size": batch_size,
                "total_frames": network.get('total_frames', 0),
                "total_events": network.get('total_events_sent', 0),
                "reduction_percent": round(network.get('network_reduction_percent', 0), 1),
                "events_per_second": round(network.get('events_per_second', 0), 2),
                "avg_batch_achieved": round(network.get('avg_batch_size_achieved', 0), 2),
                "batch_fill_ratio": round(network.get('batch_fill_ratio', 0) * 100, 1)
            })
        
        return tables
    
    def calculate_optimal_config(self, latency_threshold_ms: float = 200.0) -> Dict:
        """
        Calculate optimal configuration based on scoring.
        
        Score = (throughput_weight × throughput_ratio) 
              + (latency_weight × (1 - latency_penalty))
              + (network_weight × network_reduction/100)
        """
        if not self.experiment_results:
            return {}
        
        # Get baseline latency (batch_size=1)
        baseline_latency = None
        for result in self.experiment_results:
            if result.get('config', {}).get('batch_size') == 1:
                baseline_latency = result.get('latency', {}).get('mean_ms', 0)
                break
        
        if baseline_latency is None:
            # Use minimum latency as baseline
            baseline_latency = min(
                r.get('latency', {}).get('mean_ms', float('inf'))
                for r in self.experiment_results
            )
        
        # Calculate scores
        scores = []
        for result in self.experiment_results:
            config = result.get('config', {})
            batch_size = config.get('batch_size', 1)
            
            throughput = result.get('throughput', {})
            latency = result.get('latency', {})
            network = result.get('network_efficiency', {})
            
            throughput_ratio = throughput.get('throughput_ratio', 0)
            mean_latency = latency.get('mean_ms', 0)
            p90_latency = latency.get('p90_ms', 0)
            network_reduction = network.get('network_reduction_percent', 0)
            
            # Latency penalty (0 if at baseline, increases with higher latency)
            if baseline_latency > 0:
                latency_penalty = max(0, (mean_latency / baseline_latency) - 1)
            else:
                latency_penalty = 0
            
            # Check if latency is within acceptable threshold
            latency_acceptable = p90_latency <= latency_threshold_ms
            
            # Weighted score
            # Weights: throughput=40%, latency=30%, network=30%
            score = (
                0.4 * throughput_ratio +
                0.3 * max(0, 1 - latency_penalty) +
                0.3 * (network_reduction / 100)
            )
            
            # Penalty if latency exceeds threshold
            if not latency_acceptable:
                score *= 0.5  # Heavy penalty for exceeding latency threshold
            
            scores.append({
                "batch_size": batch_size,
                "score": round(score, 4),
                "throughput_ratio": round(throughput_ratio, 4),
                "latency_penalty": round(latency_penalty, 4),
                "network_reduction": round(network_reduction, 1),
                "mean_latency_ms": round(mean_latency, 2),
                "p90_latency_ms": round(p90_latency, 2),
                "latency_acceptable": latency_acceptable
            })
        
        # Sort by score (descending)
        scores.sort(key=lambda x: x['score'], reverse=True)
        
        return {
            "baseline_latency_ms": round(baseline_latency, 2),
            "latency_threshold_ms": latency_threshold_ms,
            "ranking": scores,
            "optimal": scores[0] if scores else None
        }
    
    def generate_analysis_text(self) -> str:
        """Generate human-readable analysis text."""
        if not self.experiment_results:
            return "No experiment results to analyze."
        
        tables = self.generate_comparison_tables()
        optimal = self.calculate_optimal_config()
        
        lines = []
        lines.append("=" * 80)
        lines.append("MICRO-BATCHING EXPERIMENT ANALYSIS REPORT")
        lines.append("=" * 80)
        lines.append(f"\nGenerated: {datetime.now().isoformat()}")
        lines.append(f"Number of experiments: {len(self.experiment_results)}")
        
        # Throughput Table
        lines.append("\n" + "─" * 80)
        lines.append("THROUGHPUT COMPARISON")
        lines.append("─" * 80)
        lines.append(f"{'Batch Size':<12} {'Overall FPS':<14} {'Mean FPS':<12} {'Std Dev':<10} {'% of Target':<12}")
        lines.append("-" * 60)
        for row in tables['throughput']:
            lines.append(
                f"{row['batch_size']:<12} "
                f"{row['overall_fps']:<14.2f} "
                f"{row['mean_batch_fps']:<12.2f} "
                f"{row['std_fps']:<10.2f} "
                f"{row['throughput_ratio']:<12.1f}%"
            )
        
        # Latency Table
        lines.append("\n" + "─" * 80)
        lines.append("LATENCY COMPARISON (milliseconds)")
        lines.append("─" * 80)
        lines.append(f"{'Batch Size':<12} {'Mean':<10} {'Median':<10} {'p90':<10} {'p99':<10} {'Std Dev':<10}")
        lines.append("-" * 62)
        for row in tables['latency']:
            lines.append(
                f"{row['batch_size']:<12} "
                f"{row['mean_ms']:<10.2f} "
                f"{row['median_ms']:<10.2f} "
                f"{row['p90_ms']:<10.2f} "
                f"{row['p99_ms']:<10.2f} "
                f"{row['std_ms']:<10.2f}"
            )
        
        # Network Efficiency Table
        lines.append("\n" + "─" * 80)
        lines.append("NETWORK EFFICIENCY COMPARISON")
        lines.append("─" * 80)
        lines.append(f"{'Batch Size':<12} {'Events Sent':<14} {'Reduction %':<14} {'Events/sec':<12} {'Batch Fill %':<12}")
        lines.append("-" * 64)
        for row in tables['network_efficiency']:
            lines.append(
                f"{row['batch_size']:<12} "
                f"{row['total_events']:<14} "
                f"{row['reduction_percent']:<14.1f} "
                f"{row['events_per_second']:<12.2f} "
                f"{row['batch_fill_ratio']:<12.1f}%"
            )
        
        # Optimal Configuration
        lines.append("\n" + "─" * 80)
        lines.append("OPTIMAL CONFIGURATION ANALYSIS")
        lines.append("─" * 80)
        lines.append(f"\nBaseline latency (batch_size=1): {optimal.get('baseline_latency_ms', 0):.2f} ms")
        lines.append(f"Latency threshold: {optimal.get('latency_threshold_ms', 200):.0f} ms")
        
        lines.append("\nRanking by composite score:")
        lines.append(f"{'Rank':<6} {'Batch':<8} {'Score':<10} {'Throughput':<12} {'Latency':<12} {'Network':<10} {'Acceptable':<10}")
        lines.append("-" * 70)
        for i, score in enumerate(optimal.get('ranking', []), 1):
            lines.append(
                f"{i:<6} "
                f"{score['batch_size']:<8} "
                f"{score['score']:<10.4f} "
                f"{score['throughput_ratio']*100:<12.1f}% "
                f"{score['mean_latency_ms']:<12.2f}ms "
                f"{score['network_reduction']:<10.1f}% "
                f"{'✓' if score['latency_acceptable'] else '✗':<10}"
            )
        
        # Final Recommendation
        optimal_config = optimal.get('optimal', {})
        if optimal_config:
            lines.append("\n" + "═" * 80)
            lines.append("RECOMMENDATION")
            lines.append("═" * 80)
            lines.append(f"""
Based on the analysis, the optimal micro-batching configuration is:

  ╔══════════════════════════════════════════════════════════════╗
  ║  OPTIMAL BATCH SIZE: {optimal_config['batch_size']}                                       ║
  ╠══════════════════════════════════════════════════════════════╣
  ║  • Score: {optimal_config['score']:.4f}                                            ║
  ║  • Throughput: {optimal_config['throughput_ratio']*100:.1f}% of target                              ║
  ║  • Mean Latency: {optimal_config['mean_latency_ms']:.2f} ms                                   ║
  ║  • p90 Latency: {optimal_config['p90_latency_ms']:.2f} ms                                    ║
  ║  • Network Reduction: {optimal_config['network_reduction']:.1f}%                                 ║
  ║  • Latency Acceptable: {'Yes' if optimal_config['latency_acceptable'] else 'No'}                                       ║
  ╚══════════════════════════════════════════════════════════════╝
""")
        
        return "\n".join(lines)
    
    def create_visualizations(self, output_dir: Optional[str] = None) -> List[str]:
        """Create visualization plots."""
        if not HAS_MATPLOTLIB:
            print("⚠️ matplotlib not available. Skipping visualizations.")
            return []
        
        if not self.experiment_results:
            return []
        
        output_dir = output_dir or os.path.join(self.results_dir, 'plots')
        os.makedirs(output_dir, exist_ok=True)
        
        created_files = []
        
        tables = self.generate_comparison_tables()
        
        # Extract data
        batch_sizes = [r['batch_size'] for r in tables['throughput']]
        throughputs = [r['overall_fps'] for r in tables['throughput']]
        latencies = [r['mean_ms'] for r in tables['latency']]
        p90_latencies = [r['p90_ms'] for r in tables['latency']]
        network_reductions = [r['reduction_percent'] for r in tables['network_efficiency']]
        
        # 1. Throughput vs Latency Trade-off
        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        color1 = 'tab:blue'
        ax1.set_xlabel('Batch Size')
        ax1.set_ylabel('Throughput (FPS)', color=color1)
        line1 = ax1.plot(batch_sizes, throughputs, 'o-', color=color1, linewidth=2, markersize=8, label='Throughput')
        ax1.tick_params(axis='y', labelcolor=color1)
        ax1.axhline(y=30, color=color1, linestyle='--', alpha=0.5, label='Target (30 FPS)')
        
        ax2 = ax1.twinx()
        color2 = 'tab:red'
        ax2.set_ylabel('Mean Latency (ms)', color=color2)
        line2 = ax2.plot(batch_sizes, latencies, 's-', color=color2, linewidth=2, markersize=8, label='Mean Latency')
        ax2.tick_params(axis='y', labelcolor=color2)
        
        ax1.set_xticks(batch_sizes)
        ax1.set_title('Throughput vs Latency Trade-off')
        ax1.grid(True, alpha=0.3)
        
        # Combined legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
        
        plt.tight_layout()
        filepath = os.path.join(output_dir, 'throughput_vs_latency.png')
        plt.savefig(filepath, dpi=150)
        plt.close()
        created_files.append(filepath)
        print(f"   ✓ Created: {filepath}")
        
        # 2. Latency Percentiles
        fig, ax = plt.subplots(figsize=(10, 6))
        
        p50 = [r['p50_ms'] for r in tables['latency']]
        p75 = [r['p75_ms'] for r in tables['latency']]
        p90 = [r['p90_ms'] for r in tables['latency']]
        p99 = [r['p99_ms'] for r in tables['latency']]
        
        x = range(len(batch_sizes))
        width = 0.2
        
        ax.bar([i - 1.5*width for i in x], p50, width, label='p50', color='green', alpha=0.8)
        ax.bar([i - 0.5*width for i in x], p75, width, label='p75', color='blue', alpha=0.8)
        ax.bar([i + 0.5*width for i in x], p90, width, label='p90', color='orange', alpha=0.8)
        ax.bar([i + 1.5*width for i in x], p99, width, label='p99', color='red', alpha=0.8)
        
        ax.set_xlabel('Batch Size')
        ax.set_ylabel('Latency (ms)')
        ax.set_title('Latency Percentile Distribution by Batch Size')
        ax.set_xticks(x)
        ax.set_xticklabels(batch_sizes)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        filepath = os.path.join(output_dir, 'latency_percentiles.png')
        plt.savefig(filepath, dpi=150)
        plt.close()
        created_files.append(filepath)
        print(f"   ✓ Created: {filepath}")
        
        # 3. Network Efficiency
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(batch_sizes, network_reductions, color='teal', alpha=0.8)
        ax.set_xlabel('Batch Size')
        ax.set_ylabel('Network Reduction (%)')
        ax.set_title('Network Efficiency Improvement by Batch Size')
        ax.set_xticks(batch_sizes)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar, val in zip(bars, network_reductions):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                   f'{val:.1f}%', ha='center', va='bottom', fontsize=10)
        
        plt.tight_layout()
        filepath = os.path.join(output_dir, 'network_efficiency.png')
        plt.savefig(filepath, dpi=150)
        plt.close()
        created_files.append(filepath)
        print(f"   ✓ Created: {filepath}")
        
        # 4. Composite Score Ranking
        optimal = self.calculate_optimal_config()
        ranking = optimal.get('ranking', [])
        
        if ranking:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            batch_sizes_ranked = [r['batch_size'] for r in ranking]
            scores = [r['score'] for r in ranking]
            colors = ['gold' if i == 0 else 'silver' if i == 1 else 'bronze' if i == 2 else 'gray' 
                     for i in range(len(ranking))]
            
            bars = ax.barh(range(len(ranking)), scores, color=colors, alpha=0.8)
            ax.set_yticks(range(len(ranking)))
            ax.set_yticklabels([f'Batch Size {bs}' for bs in batch_sizes_ranked])
            ax.set_xlabel('Composite Score')
            ax.set_title('Configuration Ranking by Composite Score')
            ax.invert_yaxis()
            ax.grid(True, alpha=0.3, axis='x')
            
            # Add score labels
            for bar, score in zip(bars, scores):
                ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
                       f'{score:.4f}', ha='left', va='center', fontsize=10)
            
            plt.tight_layout()
            filepath = os.path.join(output_dir, 'score_ranking.png')
            plt.savefig(filepath, dpi=150)
            plt.close()
            created_files.append(filepath)
            print(f"   ✓ Created: {filepath}")
        
        return created_files
    
    def save_full_report(self, output_path: Optional[str] = None) -> str:
        """Save complete analysis report to JSON."""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(
                self.results_dir,
                f'analysis_report_{timestamp}.json'
            )
        
        report = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "num_experiments": len(self.experiment_results)
            },
            "comparison_tables": self.generate_comparison_tables(),
            "optimal_analysis": self.calculate_optimal_config(),
            "source_files": [r.get('_source_file', '') for r in self.experiment_results]
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        return output_path


def main():
    parser = argparse.ArgumentParser(
        description='Analyze micro-batching experiment results'
    )
    parser.add_argument(
        '--results-dir',
        type=str,
        default=os.path.join(PROJECT_ROOT, 'data', 'experiment_results'),
        help='Directory containing experiment result files'
    )
    parser.add_argument(
        '--latency-threshold',
        type=float,
        default=200.0,
        help='Maximum acceptable p90 latency in ms (default: 200)'
    )
    parser.add_argument(
        '--create-plots',
        action='store_true',
        default=True,
        help='Create visualization plots'
    )
    parser.add_argument(
        '--output-report',
        type=str,
        default=None,
        help='Path to save the analysis report'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🔬 MICRO-BATCHING EXPERIMENT ANALYSIS")
    print("="*60)
    
    analyzer = ExperimentAnalyzer(args.results_dir)
    
    # Load results
    num_loaded = analyzer.load_results()
    
    if num_loaded == 0:
        print("\n⚠️ No experiment results found!")
        print(f"   Looking in: {args.results_dir}")
        print("\n   To run experiments first, use:")
        print("   python -m experiments.run_experiments")
        return 1
    
    # Generate and print analysis
    print("\n")
    analysis_text = analyzer.generate_analysis_text()
    print(analysis_text)
    
    # Save analysis text
    text_path = os.path.join(args.results_dir, 'analysis_report.txt')
    with open(text_path, 'w') as f:
        f.write(analysis_text)
    print(f"\n📄 Analysis text saved to: {text_path}")
    
    # Create visualizations
    if args.create_plots:
        print("\n📊 Creating visualizations...")
        created_plots = analyzer.create_visualizations()
        if created_plots:
            print(f"   Created {len(created_plots)} visualization(s)")
    
    # Save JSON report
    report_path = analyzer.save_full_report(args.output_report)
    print(f"📄 Full report saved to: {report_path}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
