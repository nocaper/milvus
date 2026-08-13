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
            'datanode_serialize': [],
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
                if 'serialize_ms' in event:
                    write_stages['datanode_serialize'].append(event.get('serialize_ms', 0))

        return {stage: self._calc_stats(durs) for stage, durs in write_stages.items()}

    def analyze_search_path(self) -> Dict:
        """Analyze search/query path latencies."""
        route_latency: Dict[str, List[float]] = defaultdict(list)
        trace_ids_by_operation: Dict[str, set] = defaultdict(set)
        segment_counts_by_trace: Dict[str, dict] = defaultdict(dict)
        cache_hits_by_operation: Dict[str, int] = defaultdict(int)
        cache_misses_by_operation: Dict[str, int] = defaultdict(int)
        segment_stats_without_counts = 0

        for event in self.traces:
            operation = event.get('operation', '')
            if operation not in ('Search', 'Query'):
                continue
            trace_id = event.get('trace_id', '')
            stage = event.get('stage', '')
            if stage in ('route', 'segment_stats'):
                trace_ids_by_operation[operation].add(trace_id)
            if stage == 'route':
                route_latency[operation].append(event.get('duration_ms', 0))
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
            elif stage == 'segment_cache_hit':
                cache_hits_by_operation[operation] += 1
            elif stage == 'segment_cache_miss':
                cache_misses_by_operation[operation] += 1

        growing_hits = 0
        sealed_hits = 0
        route_count_fallbacks = 0
        traces_with_segment_counts = 0
        for counts in segment_counts_by_trace.values():
            if 'stats_sealed' in counts or 'stats_growing' in counts:
                sealed_hits += counts.get('stats_sealed', 0)
                growing_hits += counts.get('stats_growing', 0)
                traces_with_segment_counts += 1
            else:
                sealed_hits += counts.get('route_sealed', 0)
                growing_hits += counts.get('route_growing', 0)
                if 'route_sealed' in counts or 'route_growing' in counts:
                    route_count_fallbacks += 1
                    traces_with_segment_counts += 1

        stats: Dict = {
            'total_requests': sum(len(v) for v in trace_ids_by_operation.values()),
            'search_requests': len(trace_ids_by_operation.get('Search', set())),
            'query_requests': len(trace_ids_by_operation.get('Query', set())),
            'growing_segment_hits': growing_hits,
            'sealed_segment_hits': sealed_hits,
            'traces_with_segment_counts': traces_with_segment_counts,
            'segment_stats_without_counts': segment_stats_without_counts,
            'route_count_fallbacks': route_count_fallbacks,
            'cache_hits_by_operation': dict(cache_hits_by_operation),
            'cache_misses_by_operation': dict(cache_misses_by_operation),
            'route_latency': {
                op: self._calc_stats(durs)
                for op, durs in route_latency.items()
            },
        }
        return stats

    def analyze_querynode_cache_execution(self) -> Dict:
        """Analyze per-segment execution once data is already in QueryNode memory/cache."""
        buckets = {
            'search_segment_cache_execution': [],
            'query_segment_cache_execution': [],
        }
        by_segment_type: Dict[str, List[float]] = defaultdict(list)

        for event in self.traces:
            operation = event.get('operation')
            stage = event.get('stage')
            if operation == 'Search' and stage == 'segment_search':
                duration = event.get('duration_ms', 0)
                buckets['search_segment_cache_execution'].append(duration)
                by_segment_type[str(event.get('segment_type', 'unknown'))].append(duration)
            elif operation == 'Query' and stage == 'segment_query':
                duration = event.get('duration_ms', 0)
                buckets['query_segment_cache_execution'].append(duration)
                by_segment_type[str(event.get('segment_type', 'unknown'))].append(duration)

        return {
            'stages': {name: self._calc_stats(durs) for name, durs in buckets.items()},
            'by_segment_type': {
                name: self._calc_stats(durs)
                for name, durs in sorted(by_segment_type.items())
            },
        }

    def analyze_load_operations(self) -> Dict:
        """Analyze LoadCollection/LoadPartition operations."""
        load_latencies: List[float] = []

        for trace_id, events in self.traces_by_id.items():
            if events[0].get('operation', '') not in ('LoadCollection', 'LoadPartition'):
                continue
            load_latencies.append(sum(e.get('duration_ms', 0) for e in events))

        return self._calc_stats(load_latencies)

    def _trace_request_operations(self) -> Dict[str, str]:
        """Map trace IDs to the request operation that created them."""
        trace_ops: Dict[str, str] = {}
        for event in self.traces:
            op = event.get('operation')
            stage = event.get('stage')
            if op in ('Search', 'Query') and stage in ('route', 'segment_stats', 'segment_search', 'segment_query'):
                trace_ops[event.get('trace_id', '')] = op
        return trace_ops

    def analyze_segment_loads(self) -> Dict:
        """Analyze segment loads from object storage/cold cache paths."""
        total_load: List[float] = []
        cache_miss_load: List[float] = []
        load_substages: Dict[str, List[float]] = defaultdict(list)
        by_request_operation: Dict[str, List[float]] = defaultdict(list)
        trace_ops = self._trace_request_operations()
        substage_names = {
            'load_index',
            'load_field_data',
            'load_multi_field_data',
            'load_bloom_filter',
            'deserialize_stats',
            'load_delta_logs',
            'deserialize_delta',
            'load_delta_apply',
        }

        for event in self.traces:
            if event.get('operation') != 'LoadSegment':
                continue
            stage = event.get('stage')
            duration = event.get('duration_ms', 0)
            if stage == 'total_load':
                total_load.append(duration)
            elif stage == 'segment_cache_load':
                cache_miss_load.append(duration)
                trigger = str(event.get('trigger') or trace_ops.get(event.get('trace_id', ''), 'Unknown'))
                by_request_operation[trigger].append(duration)
            elif stage in substage_names:
                load_substages[stage].append(duration)

        return {
            'total_load': self._calc_stats(total_load),
            'cache_miss_load': self._calc_stats(cache_miss_load),
            'substages': {
                stage: self._calc_stats(durs)
                for stage, durs in sorted(load_substages.items())
            },
            'cache_miss_by_request_operation': {
                op: self._calc_stats(durs)
                for op, durs in sorted(by_request_operation.items())
            },
        }

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
        cache_exec_stats = self.analyze_querynode_cache_execution()

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
        self._print_segment_load_stats(seg_stats)

        print("\n### QUERYNODE CACHE EXECUTION ANALYSIS")
        print("-" * 80)
        self._print_cache_execution_stats(cache_exec_stats)

        print("\n### BOTTLENECK SUMMARY & OPTIMIZATION OPPORTUNITIES")
        print("-" * 80)
        self._print_bottleneck_summary(write_stats, search_stats, seg_stats, cache_exec_stats)

        if HAS_PLOT_DEPS:
            self._save_to_csv(output_path)
            self._generate_plots(output_path, write_stats, search_stats, seg_stats, cache_exec_stats)
            print(f"\nCSV and plots saved to {output_path}")
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
        print(f"\nTotal search/query requests: {stats.get('total_requests', 0)}")
        print(f"Search requests:            {stats.get('search_requests', 0)}")
        print(f"Query requests:             {stats.get('query_requests', 0)}")
        print(f"Growing segment hits:       {stats.get('growing_segment_hits', 0)}")
        print(f"Sealed segment hits:        {stats.get('sealed_segment_hits', 0)}")
        print(f"Traces with segment counts: {stats.get('traces_with_segment_counts', 0)}")
        for operation in ('Search', 'Query'):
            hits = stats.get('cache_hits_by_operation', {}).get(operation, 0)
            misses = stats.get('cache_misses_by_operation', {}).get(operation, 0)
            if hits or misses:
                print(f"{operation} QueryNode cache hits/misses: {hits} / {misses}")
        if stats.get('segment_stats_without_counts', 0):
            print(
                f"Segment stats without counts: {stats['segment_stats_without_counts']} "
                "(log was produced before metadata fields were emitted, or metadata was malformed)"
            )
        if stats.get('route_count_fallbacks', 0):
            print(f"Segment counts read from route metadata: {stats['route_count_fallbacks']}")
        if (
            not stats.get('cache_hits_by_operation')
            and not stats.get('cache_misses_by_operation')
        ):
            print("Lazy-load disk cache hits/misses: No data")
        route_latency = stats.get('route_latency', {})
        for operation in ('Search', 'Query'):
            op_stats = route_latency.get(operation, {})
            if op_stats.get('count', 0) > 0:
                print(f"\n{operation} route latency:")
                self._print_stats_dict(op_stats, indent="  ")

    def _print_segment_load_stats(self, stats: Dict):
        self._print_stats_dict(stats.get('cache_miss_load', {}), "Query/Search cache-miss load from object store")
        self._print_stats_dict(stats.get('total_load', {}), "All LoadSegment total load")

        by_op = stats.get('cache_miss_by_request_operation', {})
        for operation in ('Search', 'Query', 'Unknown'):
            op_stats = by_op.get(operation, {})
            if op_stats.get('count', 0) > 0:
                self._print_stats_dict(op_stats, f"Cache-miss load during {operation}", indent="  ")

        substages = stats.get('substages', {})
        if substages:
            print("\nLoad substages:")
            for stage, stage_stats in substages.items():
                self._print_stats_dict(stage_stats, stage, indent="  ")

    def _print_cache_execution_stats(self, stats: Dict):
        stages = stats.get('stages', {})
        self._print_stats_dict(stages.get('search_segment_cache_execution', {}), "Search segment execution on QueryNode cache")
        self._print_stats_dict(stages.get('query_segment_cache_execution', {}), "Query segment execution on QueryNode cache")

        by_type = stats.get('by_segment_type', {})
        if by_type:
            print("\nBy segment type:")
            for segment_type, type_stats in by_type.items():
                self._print_stats_dict(type_stats, segment_type, indent="  ")

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

    def _print_bottleneck_summary(self, write_stats, search_stats, seg_stats, cache_exec_stats):
        print("\nKEY FINDINGS:")

        cache_miss_load = seg_stats.get('cache_miss_load', {})
        total_load = seg_stats.get('total_load', {})
        hot_search = cache_exec_stats.get('stages', {}).get('search_segment_cache_execution', {})
        hot_query = cache_exec_stats.get('stages', {}).get('query_segment_cache_execution', {})

        if cache_miss_load.get('count', 0) > 0:
            print("\n1. QUERY/SEARCH CACHE MISS LOAD FROM OBJECT STORE:")
            print(f"   - {cache_miss_load['count']} cache-miss segment loads observed")
            print(f"   - Mean latency: {cache_miss_load['mean']:.2f} ms")
            print(f"   - P95 latency:  {cache_miss_load['p95']:.2f} ms")
            print("   - This is the cold path most directly affected by avoiding object-store reads.")
        elif total_load.get('count', 0) > 0:
            print("\n1. SEGMENT LOAD FROM OBJECT STORE:")
            print(f"   - {total_load['count']} segment loads observed outside a traced query/search request")
            print(f"   - Mean latency: {total_load['mean']:.2f} ms")
            print(f"   - P95 latency:  {total_load['p95']:.2f} ms")
        else:
            print("\n1. SEGMENT LOAD FROM OBJECT STORE:")
            print("   - No LoadSegment events found.")

        if write_stats.get('s3_write', {}).get('count', 0) > 0:
            print("\n2. DATANODE S3 WRITE:")
            print(f"   - Mean latency: {write_stats['s3_write']['mean']:.2f} ms")
            if write_stats.get('datanode_serialize', {}).get('count', 0) > 0:
                print(f"   - Serialize before write mean: {write_stats['datanode_serialize']['mean']:.2f} ms")

        total = search_stats.get('total_requests', 0)
        if total > 0:
            print("\n3. SEARCH/QUERY PATTERN:")
            print(f"   - Total requests:       {total}")
            print(f"   - Search requests:      {search_stats.get('search_requests', 0)}")
            print(f"   - Query requests:       {search_stats.get('query_requests', 0)}")
            print(f"   - Growing segment hits: {search_stats.get('growing_segment_hits', 0)}")
            print(f"   - Sealed segment hits:  {search_stats.get('sealed_segment_hits', 0)}")

        if hot_search.get('count', 0) > 0 or hot_query.get('count', 0) > 0:
            print("\n4. QUERYNODE CACHE EXECUTION:")
            if hot_search.get('count', 0) > 0:
                print(f"   - Search cache executions: {hot_search['count']}, mean {hot_search['mean']:.2f} ms")
            if hot_query.get('count', 0) > 0:
                print(f"   - Query cache executions:  {hot_query['count']}, mean {hot_query['mean']:.2f} ms")

        print("\nEXPECTED OPTIMIZATION IMPACT:")
        print("   Compare cache-miss object-store load latency with QueryNode cache execution latency.")
        print("   If cache-miss load dominates, shared memory or cache reuse targets the right bottleneck.")

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

    def _generate_plots(self, output_path: Path, write_stats, search_stats, seg_stats, cache_exec_stats):
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
        load_stats = seg_stats.get('cache_miss_load', {})
        load_stages = ('segment_cache_load',)
        if load_stats.get('count', 0) == 0:
            load_stats = seg_stats.get('total_load', {})
            load_stages = ('total_load',)
        if load_stats.get('count', 0) > 0:
            durations = [
                e.get('duration_ms', 0)
                for e in self.traces
                if e.get('operation') == 'LoadSegment'
                and e.get('stage') in load_stages
            ]
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(durations, bins=30, alpha=0.7, edgecolor='black')
            ax.axvline(load_stats['mean'], color='r',      linestyle='--',
                       label=f"Mean: {load_stats['mean']:.2f} ms")
            ax.axvline(load_stats['p95'],  color='orange', linestyle='--',
                       label=f"P95: {load_stats['p95']:.2f} ms")
            ax.set_xlabel('Latency (ms)')
            ax.set_ylabel('Frequency')
            ax.set_title('Segment Object-Store Load Latency Distribution')
            ax.legend()
            plt.tight_layout()
            plt.savefig(output_path / 'segment_load_distribution.png', dpi=300)
            plt.close()

        # Growing vs Sealed
        if search_stats.get('total_requests', 0) > 0:
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
