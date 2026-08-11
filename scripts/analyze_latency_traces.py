#!/usr/bin/env python3
"""
Milvus Latency Trace Analysis Script

This script analyzes latency trace data collected from Milvus instrumentation
to identify bottlenecks and quantify optimization opportunities.

Usage:
    python analyze_latency_traces.py <trace_file.jsonl> [--output <report_dir>]
"""

import json
import sys
import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import statistics

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


class LatencyAnalyzer:
    """Analyzes latency trace data from Milvus operations"""

    def __init__(self, trace_file: str):
        self.trace_file = trace_file
        self.traces = []
        self.traces_by_id = defaultdict(list)
        self.load_traces()

    def load_traces(self):
        """Load trace events from JSONL file"""
        with open(self.trace_file, 'r') as f:
            for line in f:
                try:
                    event = json.loads(line.strip())
                    self.traces.append(event)
                    if event.get('trace_id'):
                        self.traces_by_id[event['trace_id']].append(event)
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse line: {e}", file=sys.stderr)

        print(f"Loaded {len(self.traces)} trace events from {len(self.traces_by_id)} traces")

    def analyze_write_path(self) -> Dict:
        """Analyze write path (Insert/Upsert) latencies"""
        write_stages = {
            'serialize': [],
            'mq_produce': [],
            'consume_lag': [],
            'datanode_process': [],
            's3_write': []
        }

        for trace_id, events in self.traces_by_id.items():
            operation = events[0].get('operation', '')
            if operation not in ['Insert', 'Upsert']:
                continue

            for event in events:
                stage = event.get('stage', '')
                duration = event.get('duration_ms', 0)

                if stage in write_stages:
                    write_stages[stage].append(duration)

        # Calculate statistics
        stats = {}
        for stage, durations in write_stages.items():
            if durations:
                stats[stage] = {
                    'count': len(durations),
                    'mean': statistics.mean(durations),
                    'median': statistics.median(durations),
                    'p95': self._percentile(durations, 95),
                    'p99': self._percentile(durations, 99),
                    'min': min(durations),
                    'max': max(durations),
                }
            else:
                stats[stage] = {'count': 0}

        return stats

    def analyze_search_path(self) -> Dict:
        """Analyze search/query path latencies"""
        search_stats = {
            'total_searches': 0,
            'growing_segment_hits': 0,
            'sealed_segment_hits': 0,
            'route_latency': [],
            'total_latency': [],
        }

        for trace_id, events in self.traces_by_id.items():
            operation = events[0].get('operation', '')
            if operation not in ['Search', 'Query']:
                continue

            search_stats['total_searches'] += 1

            for event in events:
                stage = event.get('stage', '')
                duration = event.get('duration_ms', 0)
                metadata = event.get('metadata', {})

                if stage == 'route':
                    search_stats['route_latency'].append(duration)
                elif stage == 'segment_stats':
                    search_stats['growing_segment_hits'] += metadata.get('growing_segments', 0)
                    search_stats['sealed_segment_hits'] += metadata.get('sealed_segments', 0)
                elif stage == 'total_load':
                    # This is a segment load triggered by search
                    if 'load_wait' not in search_stats:
                        search_stats['load_wait'] = []
                    search_stats['load_wait'].append(duration)

        # Calculate statistics
        stats = {}
        for key, values in search_stats.items():
            if isinstance(values, list) and values:
                stats[key] = {
                    'count': len(values),
                    'mean': statistics.mean(values),
                    'median': statistics.median(values),
                    'p95': self._percentile(values, 95),
                    'p99': self._percentile(values, 99),
                }
            else:
                stats[key] = values

        return stats

    def analyze_load_operations(self) -> Dict:
        """Analyze LoadCollection/LoadPartition operations"""
        load_latencies = []

        for trace_id, events in self.traces_by_id.items():
            operation = events[0].get('operation', '')
            if operation not in ['LoadCollection', 'LoadPartition']:
                continue

            total_duration = sum(e.get('duration_ms', 0) for e in events)
            load_latencies.append(total_duration)

        if load_latencies:
            return {
                'count': len(load_latencies),
                'mean': statistics.mean(load_latencies),
                'median': statistics.median(load_latencies),
                'p95': self._percentile(load_latencies, 95),
                'p99': self._percentile(load_latencies, 99),
                'min': min(load_latencies),
                'max': max(load_latencies),
            }
        return {'count': 0}

    def analyze_segment_loads(self) -> Dict:
        """Analyze individual segment load operations (key optimization target)"""
        segment_loads = []

        for event in self.traces:
            if event.get('operation') == 'LoadSegment' and event.get('stage') == 'total_load':
                duration = event.get('duration_ms', 0)
                metadata = event.get('metadata', {})
                segment_loads.append({
                    'duration_ms': duration,
                    'segment_id': metadata.get('segment_id'),
                    'num_rows': metadata.get('num_rows'),
                    'segment_type': metadata.get('segment_type'),
                })

        if segment_loads:
            durations = [s['duration_ms'] for s in segment_loads]
            return {
                'count': len(segment_loads),
                'mean': statistics.mean(durations),
                'median': statistics.median(durations),
                'p95': self._percentile(durations, 95),
                'p99': self._percentile(durations, 99),
                'min': min(durations),
                'max': max(durations),
                'details': segment_loads,
            }
        return {'count': 0}

    def generate_report(self, output_dir: Optional[str] = None):
        """Generate comprehensive analysis report"""
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = Path('.')

        print("\n" + "="*80)
        print("MILVUS LATENCY ANALYSIS REPORT")
        print("="*80)

        # Write Path Analysis
        print("\n### WRITE PATH ANALYSIS (Insert/Upsert)")
        print("-" * 80)
        write_stats = self.analyze_write_path()
        self._print_stage_stats(write_stats)

        # Search Path Analysis
        print("\n### SEARCH/QUERY PATH ANALYSIS")
        print("-" * 80)
        search_stats = self.analyze_search_path()
        self._print_search_stats(search_stats)

        # Load Operations Analysis
        print("\n### LOAD OPERATIONS ANALYSIS")
        print("-" * 80)
        load_stats = self.analyze_load_operations()
        self._print_stats_dict(load_stats, "LoadCollection/LoadPartition")

        # Segment Load Analysis (KEY OPTIMIZATION TARGET)
        print("\n### SEGMENT LOAD ANALYSIS (OPTIMIZATION TARGET)")
        print("-" * 80)
        segment_load_stats = self.analyze_segment_loads()
        self._print_stats_dict(segment_load_stats, "Segment Load from S3")

        # Bottleneck Summary
        print("\n### BOTTLENECK SUMMARY & OPTIMIZATION OPPORTUNITIES")
        print("-" * 80)
        self._print_bottleneck_summary(write_stats, search_stats, segment_load_stats)

        # Save detailed data to CSV
        self._save_to_csv(output_path)

        # Generate plots
        self._generate_plots(output_path, write_stats, search_stats, segment_load_stats)

        print(f"\n✓ Report saved to {output_path}")
        print(f"  - CSV data: latency_analysis.csv")
        print(f"  - Plots: *.png")

    def _print_stage_stats(self, stats: Dict):
        """Print statistics for write path stages"""
        for stage, data in stats.items():
            if data.get('count', 0) > 0:
                print(f"\n{stage}:")
                print(f"  Count:    {data['count']}")
                print(f"  Mean:     {data['mean']:.2f} ms")
                print(f"  Median:   {data['median']:.2f} ms")
                print(f"  P95:      {data['p95']:.2f} ms")
                print(f"  P99:      {data['p99']:.2f} ms")
                print(f"  Min/Max:  {data['min']:.2f} / {data['max']:.2f} ms")

    def _print_search_stats(self, stats: Dict):
        """Print search/query statistics"""
        print(f"\nTotal searches: {stats.get('total_searches', 0)}")
        print(f"Growing segment hits: {stats.get('growing_segment_hits', 0)}")
        print(f"Sealed segment hits: {stats.get('sealed_segment_hits', 0)}")

        if 'route_latency' in stats and isinstance(stats['route_latency'], dict):
            print(f"\nRoute latency:")
            self._print_stats_dict(stats['route_latency'], indent="  ")

        if 'load_wait' in stats:
            print(f"\nLoad wait (searches that triggered segment load):")
            self._print_stats_dict(stats['load_wait'], indent="  ")

    def _print_stats_dict(self, data: Dict, label: str = "", indent: str = ""):
        """Print statistics dictionary"""
        if data.get('count', 0) > 0:
            if label:
                print(f"{indent}{label}:")
            print(f"{indent}  Count:    {data['count']}")
            print(f"{indent}  Mean:     {data.get('mean', 0):.2f} ms")
            print(f"{indent}  Median:   {data.get('median', 0):.2f} ms")
            print(f"{indent}  P95:      {data.get('p95', 0):.2f} ms")
            print(f"{indent}  P99:      {data.get('p99', 0):.2f} ms")
            if 'min' in data and 'max' in data:
                print(f"{indent}  Min/Max:  {data['min']:.2f} / {data['max']:.2f} ms")
        else:
            print(f"{indent}{label}: No data")

    def _print_bottleneck_summary(self, write_stats, search_stats, segment_load_stats):
        """Print bottleneck analysis and optimization opportunities"""
        print("\n🎯 KEY FINDINGS:")

        # Segment load bottleneck (main optimization target)
        if segment_load_stats.get('count', 0) > 0:
            mean_load = segment_load_stats['mean']
            p95_load = segment_load_stats['p95']
            print(f"\n1. SEGMENT LOAD FROM S3 (Primary Optimization Target):")
            print(f"   - {segment_load_stats['count']} segment loads observed")
            print(f"   - Mean latency: {mean_load:.2f} ms")
            print(f"   - P95 latency:  {p95_load:.2f} ms")
            print(f"   💡 OPTIMIZATION: Shared memory pool can eliminate this latency")
            print(f"      Expected QPS improvement: ~{(1000/mean_load)*segment_load_stats['count']:.1f} ops/sec")

        # Write path analysis
        if write_stats.get('s3_write', {}).get('count', 0) > 0:
            s3_write_mean = write_stats['s3_write']['mean']
            print(f"\n2. DATANODE S3 WRITE:")
            print(f"   - Mean latency: {s3_write_mean:.2f} ms")
            print(f"   💡 OPTIMIZATION: Can be parallelized with shared memory push")

        # Growing vs Sealed analysis
        total_searches = search_stats.get('total_searches', 0)
        if total_searches > 0:
            growing_hits = search_stats.get('growing_segment_hits', 0)
            sealed_hits = search_stats.get('sealed_segment_hits', 0)
            print(f"\n3. SEARCH PATTERN ANALYSIS:")
            print(f"   - Total searches: {total_searches}")
            print(f"   - Growing segment queries: {growing_hits} (fast path, no S3)")
            print(f"   - Sealed segment queries:  {sealed_hits} (may need S3 if not loaded)")
            print(f"   💡 Sealed segment queries benefit most from shared memory optimization")

        print("\n📊 EXPECTED OPTIMIZATION IMPACT:")
        print("   With shared memory pool between DataNode and QueryNode:")
        print("   ✓ Eliminate S3 read latency for segment loads")
        print("   ✓ Eliminate deserialization overhead (direct memory access)")
        print("   ✓ LoadCollection/LoadPartition latency → near zero")
        print("   ✓ Sealed segment queries: latency reduction = segment_load_latency")

    def _save_to_csv(self, output_path: Path):
        """Save raw trace data to CSV for further analysis"""
        df = pd.DataFrame(self.traces)
        csv_file = output_path / 'latency_analysis.csv'
        df.to_csv(csv_file, index=False)

        # Also save per-trace summary
        trace_summary = []
        for trace_id, events in self.traces_by_id.items():
            operation = events[0].get('operation', 'Unknown')
            total_duration = sum(e.get('duration_ms', 0) for e in events)
            trace_summary.append({
                'trace_id': trace_id,
                'operation': operation,
                'num_stages': len(events),
                'total_duration_ms': total_duration,
            })

        df_summary = pd.DataFrame(trace_summary)
        summary_file = output_path / 'trace_summary.csv'
        df_summary.to_csv(summary_file, index=False)

    def _generate_plots(self, output_path: Path, write_stats, search_stats, segment_load_stats):
        """Generate visualization plots"""
        sns.set_style("whitegrid")

        # Plot 1: Write path stage breakdown
        if any(s.get('count', 0) > 0 for s in write_stats.values()):
            fig, ax = plt.subplots(figsize=(12, 6))
            stages = []
            means = []
            p95s = []

            for stage, data in write_stats.items():
                if data.get('count', 0) > 0:
                    stages.append(stage)
                    means.append(data['mean'])
                    p95s.append(data['p95'])

            x = range(len(stages))
            width = 0.35
            ax.bar([i - width/2 for i in x], means, width, label='Mean', alpha=0.8)
            ax.bar([i + width/2 for i in x], p95s, width, label='P95', alpha=0.8)

            ax.set_xlabel('Stage')
            ax.set_ylabel('Latency (ms)')
            ax.set_title('Write Path Latency Breakdown')
            ax.set_xticks(x)
            ax.set_xticklabels(stages, rotation=45, ha='right')
            ax.legend()
            plt.tight_layout()
            plt.savefig(output_path / 'write_path_latency.png', dpi=300)
            plt.close()

        # Plot 2: Segment load latency distribution (KEY!)
        if segment_load_stats.get('count', 0) > 0:
            durations = [s['duration_ms'] for s in segment_load_stats.get('details', [])]

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(durations, bins=30, alpha=0.7, edgecolor='black')
            ax.axvline(segment_load_stats['mean'], color='r', linestyle='--',
                      label=f'Mean: {segment_load_stats["mean"]:.2f}ms')
            ax.axvline(segment_load_stats['p95'], color='orange', linestyle='--',
                      label=f'P95: {segment_load_stats["p95"]:.2f}ms')
            ax.set_xlabel('Latency (ms)')
            ax.set_ylabel('Frequency')
            ax.set_title('Segment Load Latency Distribution (Optimization Target)')
            ax.legend()
            plt.tight_layout()
            plt.savefig(output_path / 'segment_load_distribution.png', dpi=300)
            plt.close()

        # Plot 3: Growing vs Sealed segment comparison
        if search_stats.get('total_searches', 0) > 0:
            fig, ax = plt.subplots(figsize=(8, 6))
            categories = ['Growing\nSegments', 'Sealed\nSegments']
            counts = [
                search_stats.get('growing_segment_hits', 0),
                search_stats.get('sealed_segment_hits', 0)
            ]
            colors = ['#2ecc71', '#e74c3c']

            bars = ax.bar(categories, counts, color=colors, alpha=0.7, edgecolor='black')
            ax.set_ylabel('Number of Segment Queries')
            ax.set_title('Growing vs Sealed Segment Query Distribution')

            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom')

            plt.tight_layout()
            plt.savefig(output_path / 'segment_type_distribution.png', dpi=300)
            plt.close()

    @staticmethod
    def _percentile(data: List[float], percentile: float) -> float:
        """Calculate percentile value"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Milvus latency trace data for bottleneck identification'
    )
    parser.add_argument('trace_file', help='Path to trace JSONL file')
    parser.add_argument('--output', '-o', default='./latency_report',
                       help='Output directory for report and plots')

    args = parser.parse_args()

    if not Path(args.trace_file).exists():
        print(f"Error: Trace file not found: {args.trace_file}", file=sys.stderr)
        sys.exit(1)

    analyzer = LatencyAnalyzer(args.trace_file)
    analyzer.generate_report(args.output)


if __name__ == '__main__':
    main()
