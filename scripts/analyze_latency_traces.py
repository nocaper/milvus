#!/usr/bin/env python3
"""
Milvus Latency Trace Analysis Script

Parses [LATENCY_TRACE] lines written by Milvus directly to stdout and
analyzes them to identify bottlenecks and quantify optimization opportunities.

Log line format emitted by the tracer:
  [LATENCY_TRACE] trace_id=<id> operation=<op> stage=<stage> component=<comp> duration_ms=<ms>

Usage:
    # From a captured log file:
    python analyze_latency_traces.py milvus.log

    # From stdin (pipe):
    ./milvus run 2>&1 | python analyze_latency_traces.py -

    # With report output directory:
    python analyze_latency_traces.py milvus.log --output ./report
"""

import re
import sys
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
import statistics

# Optional visualization dependencies
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_PLOT_DEPS = True
except ImportError:
    HAS_PLOT_DEPS = False

# Matches lines like:
#   [LATENCY_TRACE] trace_id=abc operation=Insert stage=serialize component=proxy duration_ms=1.23
#   [LATENCY_TRACE] trace_id=abc operation=Search stage=segment_stats component=querynode duration_ms=0.00 sealed_segments=5 growing_segments=2
_TRACE_RE = re.compile(
    r'\[LATENCY_TRACE\]\s+'
    r'trace_id=(\S*)\s+'
    r'operation=(\S+)\s+'
    r'stage=(\S+)\s+'
    r'component=(\S+)\s+'
    r'duration_ms=([\d.]+)'
    r'(.*)'  # Capture remaining line for metadata parsing
)


def parse_trace_line(line: str) -> Optional[dict]:
    """Return a dict for a [LATENCY_TRACE] line, or None if the line doesn't match."""
    m = _TRACE_RE.search(line)
    if not m:
        return None

    result = {
        'trace_id':   m.group(1),
        'operation':  m.group(2),
        'stage':      m.group(3),
        'component':  m.group(4),
        'duration_ms': float(m.group(5)),
    }

    # Parse metadata fields from remaining text (group 6)
    metadata_text = m.group(6).strip()
    if metadata_text:
        # Match key=value pairs in the metadata section. Values may be numeric
        # or strings such as channel names and segment levels.
        metadata_re = re.compile(r'(\w+)=((?:\[[^\]]*\])|"[^"]*"|\S+)')
        for match in metadata_re.finditer(metadata_text):
            key = match.group(1)
            value = match.group(2).strip('"')
            # Try to parse as int first, then float
            try:
                result[key] = int(value)
            except ValueError:
                try:
                    result[key] = float(value)
                except ValueError:
                    result[key] = value

    return result


class LatencyAnalyzer:
    """Analyzes [LATENCY_TRACE] log lines from Milvus stdout output."""

    def __init__(self):
        self.traces: List[dict] = []
        self.traces_by_id: Dict[str, List[dict]] = defaultdict(list)
        self.trace_lines_seen = 0
        self.unparsed_trace_lines = 0

    def load_from_source(self, source: str):
        """Load trace events from a log file path, or '-' for stdin."""
        if source == '-':
            src = sys.stdin
            close_after = False
        else:
            src = open(source, 'r', encoding='utf-8', errors='replace')
            close_after = True

        try:
            for line in src:
                if '[LATENCY_TRACE]' in line:
                    self.trace_lines_seen += 1
                event = parse_trace_line(line)
                if event:
                    if not event.get('trace_id'):
                        event['trace_id'] = f"missing-trace-{len(self.traces)}"
                    self.traces.append(event)
                    self.traces_by_id[event['trace_id']].append(event)
                elif '[LATENCY_TRACE]' in line:
                    self.unparsed_trace_lines += 1
        finally:
            if close_after:
                src.close()

        print(f"Loaded {len(self.traces)} trace events from {len(self.traces_by_id)} traces")
        if self.unparsed_trace_lines:
            print(
                f"Warning: skipped {self.unparsed_trace_lines} malformed [LATENCY_TRACE] lines "
                f"out of {self.trace_lines_seen}",
                file=sys.stderr,
            )

    # ------------------------------------------------------------------
    # Analysis methods
    # ------------------------------------------------------------------

    def analyze_write_path(self) -> Dict:
        """Analyze write path (Insert/Upsert) latencies."""
        write_stages = {
            'serialize': [],
            'mq_produce': [],
            'consume_lag': [],
            'datanode_process': [],
            's3_write': [],
        }

        for event in self.traces:
            stage = event.get('stage', '')
            operation = event.get('operation', '')
            if stage in ('serialize', 'mq_produce') and operation in ('Insert', 'Upsert'):
                write_stages[stage].append(event.get('duration_ms', 0))
            elif stage in ('consume_lag', 'datanode_process'):
                write_stages[stage].append(event.get('duration_ms', 0))
            elif stage == 's3_write':
                write_stages[stage].append(event.get('duration_ms', 0))

        return {stage: self._calc_stats(durs) for stage, durs in write_stages.items()}

    def analyze_search_path(self) -> Dict:
        """Analyze search/query path latencies."""
        route_latency: List[float] = []
        search_trace_ids = set()
        segment_counts_by_trace: Dict[str, dict] = defaultdict(dict)
        segment_stats_without_counts = 0

        for event in self.traces:
            if event.get('operation', '') not in ('Search', 'Query'):
                continue
            trace_id = event.get('trace_id', '')
            search_trace_ids.add(trace_id)
            stage = event.get('stage', '')
            if stage == 'route':
                route_latency.append(event.get('duration_ms', 0))
                if 'sealed_count' in event or 'growing_count' in event:
                    segment_counts_by_trace[trace_id]['route_sealed'] = event.get('sealed_count', 0)
                    segment_counts_by_trace[trace_id]['route_growing'] = event.get('growing_count', 0)
            elif stage == 'segment_stats':
                if 'growing_segments' not in event and 'sealed_segments' not in event:
                    segment_stats_without_counts += 1
                if 'sealed_segments' in event:
                    segment_counts_by_trace[trace_id]['stats_sealed'] = event.get('sealed_segments', 0)
                if 'growing_segments' in event:
                    segment_counts_by_trace[trace_id]['stats_growing'] = event.get('growing_segments', 0)

        growing_hits = 0
        sealed_hits = 0
        route_count_fallbacks = 0
        for counts in segment_counts_by_trace.values():
            if 'stats_sealed' in counts or 'stats_growing' in counts:
                sealed_hits += counts.get('stats_sealed', 0)
                growing_hits += counts.get('stats_growing', 0)
            else:
                sealed_hits += counts.get('route_sealed', 0)
                growing_hits += counts.get('route_growing', 0)
                if 'route_sealed' in counts or 'route_growing' in counts:
                    route_count_fallbacks += 1

        stats: Dict = {
            'total_searches': len(search_trace_ids) if search_trace_ids else len(route_latency),
            'growing_segment_hits': growing_hits,
            'sealed_segment_hits': sealed_hits,
            'segment_stats_without_counts': segment_stats_without_counts,
            'route_count_fallbacks': route_count_fallbacks,
        }
        if route_latency:
            stats['route_latency'] = self._calc_stats(route_latency)
        return stats

    def analyze_load_operations(self) -> Dict:
        """Analyze LoadCollection/LoadPartition operations."""
        load_latencies: List[float] = []

        for trace_id, events in self.traces_by_id.items():
            if events[0].get('operation', '') not in ('LoadCollection', 'LoadPartition'):
                continue
            load_latencies.append(sum(e.get('duration_ms', 0) for e in events))

        return self._calc_stats(load_latencies)

    def analyze_segment_loads(self) -> Dict:
        """Analyze individual segment load operations (key optimization target)."""
        durations: List[float] = []

        for event in self.traces:
            if event.get('operation') == 'LoadSegment' and event.get('stage') == 'total_load':
                durations.append(event.get('duration_ms', 0))

        return self._calc_stats(durations)

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

    def generate_report(self, output_dir: Optional[str] = None):
        """Generate comprehensive analysis report."""
        output_path = Path(output_dir) if output_dir else Path('.')
        if output_dir:
            output_path.mkdir(parents=True, exist_ok=True)

        print("\n" + "=" * 80)
        print("MILVUS LATENCY ANALYSIS REPORT")
        print("=" * 80)

        write_stats = self.analyze_write_path()
        search_stats = self.analyze_search_path()
        load_stats = self.analyze_load_operations()
        seg_stats = self.analyze_segment_loads()

        print("\n### WRITE PATH ANALYSIS (Insert/Upsert)")
        print("-" * 80)
        self._print_stage_stats(write_stats)

        print("\n### SEARCH/QUERY PATH ANALYSIS")
        print("-" * 80)
        self._print_search_stats(search_stats)

        print("\n### LOAD OPERATIONS ANALYSIS")
        print("-" * 80)
        self._print_stats_dict(load_stats, "LoadCollection/LoadPartition")

        print("\n### SEGMENT LOAD ANALYSIS (OPTIMIZATION TARGET)")
        print("-" * 80)
        self._print_stats_dict(seg_stats, "Segment Load from S3")

        print("\n### BOTTLENECK SUMMARY & OPTIMIZATION OPPORTUNITIES")
        print("-" * 80)
        self._print_bottleneck_summary(write_stats, search_stats, seg_stats)

        if HAS_PLOT_DEPS:
            self._save_to_csv(output_path)
            self._generate_plots(output_path, write_stats, search_stats, seg_stats)
            print(f"\n✓ CSV and plots saved to {output_path}")
        else:
            print("\n(Install pandas/matplotlib/seaborn for CSV export and plots)")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _calc_stats(durations: List[float]) -> Dict:
        if not durations:
            return {'count': 0}
        return {
            'count':  len(durations),
            'mean':   statistics.mean(durations),
            'median': statistics.median(durations),
            'p95':    LatencyAnalyzer._percentile(durations, 95),
            'p99':    LatencyAnalyzer._percentile(durations, 99),
            'min':    min(durations),
            'max':    max(durations),
        }

    def _print_stage_stats(self, stats: Dict):
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
        print(f"\nTotal searches: {stats.get('total_searches', 0)}")
        print(f"Growing segment hits: {stats.get('growing_segment_hits', 0)}")
        print(f"Sealed segment hits:  {stats.get('sealed_segment_hits', 0)}")
        if stats.get('segment_stats_without_counts', 0):
            print(
                f"Segment stats without counts: {stats['segment_stats_without_counts']} "
                "(log was produced before metadata fields were emitted, or metadata was malformed)"
            )
        if stats.get('route_count_fallbacks', 0):
            print(f"Segment counts read from route metadata: {stats['route_count_fallbacks']}")
        if isinstance(stats.get('route_latency'), dict):
            print("\nRoute latency:")
            self._print_stats_dict(stats['route_latency'], indent="  ")

    def _print_stats_dict(self, data: Dict, label: str = "", indent: str = ""):
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

    def _print_bottleneck_summary(self, write_stats, search_stats, seg_stats):
        print("\n🎯 KEY FINDINGS:")

        if seg_stats.get('count', 0) > 0:
            mean_load = seg_stats['mean']
            p95_load  = seg_stats['p95']
            print(f"\n1. SEGMENT LOAD FROM S3 (Primary Optimization Target):")
            print(f"   - {seg_stats['count']} segment loads observed")
            print(f"   - Mean latency: {mean_load:.2f} ms")
            print(f"   - P95 latency:  {p95_load:.2f} ms")
            print(f"   💡 OPTIMIZATION: Shared memory pool can eliminate this latency")
            if mean_load > 0:
                print(f"      Expected QPS improvement: ~{(1000/mean_load)*seg_stats['count']:.1f} ops/sec")

        if write_stats.get('s3_write', {}).get('count', 0) > 0:
            print(f"\n2. DATANODE S3 WRITE:")
            print(f"   - Mean latency: {write_stats['s3_write']['mean']:.2f} ms")
            print(f"   💡 OPTIMIZATION: Can be parallelized with shared memory push")

        total = search_stats.get('total_searches', 0)
        if total > 0:
            print(f"\n3. SEARCH PATTERN ANALYSIS:")
            print(f"   - Total searches:          {total}")
            print(f"   - Growing segment queries: {search_stats.get('growing_segment_hits', 0)}")
            print(f"   - Sealed segment queries:  {search_stats.get('sealed_segment_hits', 0)}")
            print(f"   💡 Sealed segment queries benefit most from shared memory optimization")

        print("\n📊 EXPECTED OPTIMIZATION IMPACT:")
        print("   With shared memory pool between DataNode and QueryNode:")
        print("   ✓ Eliminate S3 read latency for segment loads")
        print("   ✓ Eliminate deserialization overhead (direct memory access)")
        print("   ✓ LoadCollection/LoadPartition latency → near zero")
        print("   ✓ Sealed segment queries: latency reduction = segment_load_latency")

    def _save_to_csv(self, output_path: Path):
        df = pd.DataFrame(self.traces)
        df.to_csv(output_path / 'latency_analysis.csv', index=False)

        trace_summary = [
            {
                'trace_id':         tid,
                'operation':        events[0].get('operation', 'Unknown'),
                'num_stages':       len(events),
                'total_duration_ms': sum(e.get('duration_ms', 0) for e in events),
            }
            for tid, events in self.traces_by_id.items()
        ]
        pd.DataFrame(trace_summary).to_csv(output_path / 'trace_summary.csv', index=False)

    def _generate_plots(self, output_path: Path, write_stats, search_stats, seg_stats):
        sns.set_style("whitegrid")

        # Write path stage breakdown
        stages = [(s, d) for s, d in write_stats.items() if d.get('count', 0) > 0]
        if stages:
            fig, ax = plt.subplots(figsize=(12, 6))
            names = [s for s, _ in stages]
            means = [d['mean'] for _, d in stages]
            p95s  = [d['p95']  for _, d in stages]
            x = range(len(names))
            w = 0.35
            ax.bar([i - w/2 for i in x], means, w, label='Mean', alpha=0.8)
            ax.bar([i + w/2 for i in x], p95s,  w, label='P95',  alpha=0.8)
            ax.set_xticks(x)
            ax.set_xticklabels(names, rotation=45, ha='right')
            ax.set_xlabel('Stage')
            ax.set_ylabel('Latency (ms)')
            ax.set_title('Write Path Latency Breakdown')
            ax.legend()
            plt.tight_layout()
            plt.savefig(output_path / 'write_path_latency.png', dpi=300)
            plt.close()

        # Segment load distribution
        if seg_stats.get('count', 0) > 0:
            durations = [
                e.get('duration_ms', 0)
                for e in self.traces
                if e.get('operation') == 'LoadSegment' and e.get('stage') == 'total_load'
            ]
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(durations, bins=30, alpha=0.7, edgecolor='black')
            ax.axvline(seg_stats['mean'], color='r',      linestyle='--',
                       label=f"Mean: {seg_stats['mean']:.2f} ms")
            ax.axvline(seg_stats['p95'],  color='orange', linestyle='--',
                       label=f"P95: {seg_stats['p95']:.2f} ms")
            ax.set_xlabel('Latency (ms)')
            ax.set_ylabel('Frequency')
            ax.set_title('Segment Load Latency Distribution (Optimization Target)')
            ax.legend()
            plt.tight_layout()
            plt.savefig(output_path / 'segment_load_distribution.png', dpi=300)
            plt.close()

        # Growing vs Sealed
        if search_stats.get('total_searches', 0) > 0:
            fig, ax = plt.subplots(figsize=(8, 6))
            cats   = ['Growing\nSegments', 'Sealed\nSegments']
            counts = [search_stats.get('growing_segment_hits', 0),
                      search_stats.get('sealed_segment_hits',  0)]
            bars = ax.bar(cats, counts, color=['#2ecc71', '#e74c3c'], alpha=0.7, edgecolor='black')
            ax.set_ylabel('Number of Segment Queries')
            ax.set_title('Growing vs Sealed Segment Query Distribution')
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2., h,
                        f'{int(h)}', ha='center', va='bottom')
            plt.tight_layout()
            plt.savefig(output_path / 'segment_type_distribution.png', dpi=300)
            plt.close()

    @staticmethod
    def _percentile(data: List[float], pct: float) -> float:
        if not data:
            return 0.0
        sd = sorted(data)
        idx = int(len(sd) * pct / 100)
        return sd[min(idx, len(sd) - 1)]


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Milvus [LATENCY_TRACE] log lines for bottleneck identification'
    )
    parser.add_argument(
        'log_source',
        nargs='?',
        default='-',
        help='Log file to read, or "-" to read from stdin (default: stdin)',
    )
    parser.add_argument(
        '--output', '-o',
        default='./latency_report',
        help='Output directory for report and plots (default: ./latency_report)',
    )
    args = parser.parse_args()

    if args.log_source != '-' and not Path(args.log_source).exists():
        print(f"Error: log file not found: {args.log_source}", file=sys.stderr)
        sys.exit(1)

    analyzer = LatencyAnalyzer()
    analyzer.load_from_source(args.log_source)
    analyzer.generate_report(args.output)


if __name__ == '__main__':
    main()
