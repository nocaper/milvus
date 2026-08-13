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
        cache_wait_latency: Dict[str, List[float]] = defaultdict(list)
        cache_wait_by_result: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        cache_wait_by_trace: Dict[str, dict] = defaultdict(lambda: {
            'operation': 'Unknown',
            'durations': [],
            'segments': set(),
            'miss_waits': [],
            'hit_waits': [],
            'error_waits': [],
        })
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
            elif stage == 'segment_cache_wait':
                trace_ids_by_operation[operation].add(trace_id)
                duration = event.get('duration_ms', 0)
                status = str(event.get('status', 'ok'))
                cache_miss = self._is_truthy(event.get('cache_miss'))
                result = 'cache_miss' if cache_miss else 'cache_hit'
                if status != 'ok':
                    result = 'error'
                cache_wait_latency[operation].append(duration)
                cache_wait_by_result[operation][result].append(duration)

                request = cache_wait_by_trace[trace_id]
                request['operation'] = operation
                request['durations'].append(duration)
                if 'segment_id' in event:
                    request['segments'].add(event.get('segment_id'))
                if result == 'cache_miss':
                    request['miss_waits'].append(duration)
                elif result == 'cache_hit':
                    request['hit_waits'].append(duration)
                else:
                    request['error_waits'].append(duration)

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
            'cache_wait_latency': {
                op: self._calc_stats(durs)
                for op, durs in sorted(cache_wait_latency.items())
            },
            'cache_wait_by_result': {
                op: {
                    result: self._calc_stats(durs)
                    for result, durs in sorted(results.items())
                }
                for op, results in sorted(cache_wait_by_result.items())
            },
            'cache_wait_request_overhead': self._build_cache_wait_request_overhead(cache_wait_by_trace),
            'route_latency': {
                op: self._calc_stats(durs)
                for op, durs in route_latency.items()
            },
        }
        return stats

    def _build_cache_wait_request_overhead(self, cache_wait_by_trace: Dict[str, dict]) -> Dict:
        """Summarize request-visible lazy-load cache wait from per-segment wait events."""
        grouped: Dict[str, dict] = defaultdict(lambda: {
            'per_request_max_wait': [],
            'per_request_sum_wait': [],
            'segments_per_request': [],
            'requests_with_miss_wait': 0,
            'requests_with_hit_wait': 0,
            'requests_with_error_wait': 0,
            'requests_wait_gt_1ms': 0,
            'requests_wait_gt_10ms': 0,
            'requests_wait_gt_100ms': 0,
            'requests_wait_gt_1000ms': 0,
        })

        for request in cache_wait_by_trace.values():
            durations = request.get('durations', [])
            if not durations:
                continue
            operation = request.get('operation', 'Unknown')
            max_wait = max(durations)
            grouped[operation]['per_request_max_wait'].append(max_wait)
            grouped[operation]['per_request_sum_wait'].append(sum(durations))
            grouped[operation]['segments_per_request'].append(len(request.get('segments', set())) or len(durations))
            if request.get('miss_waits'):
                grouped[operation]['requests_with_miss_wait'] += 1
            if request.get('hit_waits'):
                grouped[operation]['requests_with_hit_wait'] += 1
            if request.get('error_waits'):
                grouped[operation]['requests_with_error_wait'] += 1
            if max_wait > 1:
                grouped[operation]['requests_wait_gt_1ms'] += 1
            if max_wait > 10:
                grouped[operation]['requests_wait_gt_10ms'] += 1
            if max_wait > 100:
                grouped[operation]['requests_wait_gt_100ms'] += 1
            if max_wait > 1000:
                grouped[operation]['requests_wait_gt_1000ms'] += 1

        return {
            operation: {
                'requests_with_cache_wait': len(data['per_request_max_wait']),
                'requests_with_miss_wait': data['requests_with_miss_wait'],
                'requests_with_hit_wait': data['requests_with_hit_wait'],
                'requests_with_error_wait': data['requests_with_error_wait'],
                'requests_wait_gt_1ms': data['requests_wait_gt_1ms'],
                'requests_wait_gt_10ms': data['requests_wait_gt_10ms'],
                'requests_wait_gt_100ms': data['requests_wait_gt_100ms'],
                'requests_wait_gt_1000ms': data['requests_wait_gt_1000ms'],
                'segments_per_request': self._calc_stats(data['segments_per_request']),
                'per_request_max_wait': self._calc_stats(data['per_request_max_wait']),
                'per_request_sum_wait': self._calc_stats(data['per_request_sum_wait']),
            }
            for operation, data in sorted(grouped.items())
        }

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
        load_stages: Dict[str, List[float]] = defaultdict(list)
        status_counts: Dict[str, int] = defaultdict(int)

        for event in self.traces:
            operation = event.get('operation', '')
            if operation not in ('LoadCollection', 'LoadPartition'):
                continue
            stage = event.get('stage', '')
            if stage not in ('querycoord_submit', 'load_start', 'load_complete', 'load_timeout', 'load_canceled'):
                continue
            load_stages[f'{operation}/{stage}'].append(event.get('duration_ms', 0))
            if 'status' in event:
                status_counts[f"{operation}/{stage}/{event.get('status')}"] += 1

        return {
            'stages': {
                stage: self._calc_stats(durs)
                for stage, durs in sorted(load_stages.items())
            },
            'status_counts': dict(sorted(status_counts.items())),
        }

    def _trace_request_operations(self) -> Dict[str, str]:
        """Map trace IDs to the request operation that created them."""
        trace_ops: Dict[str, str] = {}
        for event in self.traces:
            op = event.get('operation')
            stage = event.get('stage')
            if op in ('Search', 'Query') and stage in ('route', 'segment_stats', 'segment_search', 'segment_query'):
                trace_ops[event.get('trace_id', '')] = op
            elif op in ('LoadCollection', 'LoadPartition') and stage in (
                'querycoord_submit',
                'load_start',
                'load_complete',
                'load_timeout',
                'load_canceled',
            ):
                trace_ops[event.get('trace_id', '')] = op
        return trace_ops

    def analyze_segment_loads(self) -> Dict:
        """Analyze segment loads from object storage/cold cache paths."""
        total_load: List[float] = []
        cache_miss_load: List[float] = []
        load_substages: Dict[str, List[float]] = defaultdict(list)
        total_load_by_operation: Dict[str, List[float]] = defaultdict(list)
        by_request_operation: Dict[str, List[float]] = defaultdict(list)
        request_cache_loads: Dict[str, dict] = defaultdict(lambda: {
            'operation': 'Unknown',
            'durations': [],
            'segments': set(),
        })
        file_breakdown_events: Dict[str, List[dict]] = defaultdict(list)
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
                trigger = trace_ops.get(event.get('trace_id', ''), 'Unknown')
                total_load_by_operation[trigger].append(duration)
            elif stage == 'segment_cache_load':
                cache_miss_load.append(duration)
                trigger = str(event.get('trigger') or trace_ops.get(event.get('trace_id', ''), 'Unknown'))
                by_request_operation[trigger].append(duration)
                trace_id = event.get('trace_id', '')
                request_cache_loads[trace_id]['operation'] = trigger
                request_cache_loads[trace_id]['durations'].append(duration)
                if 'segment_id' in event:
                    request_cache_loads[trace_id]['segments'].add(event.get('segment_id'))
            elif stage in substage_names:
                load_substages[stage].append(duration)
                file_breakdown_events[stage].append(event)

        request_s3_overhead: Dict[str, dict] = {}
        request_groups: Dict[str, dict] = defaultdict(lambda: {
            'request_sum_work': [],
            'request_max_wait': [],
            'segments_per_request': [],
        })
        for request in request_cache_loads.values():
            durations = request['durations']
            if not durations:
                continue
            operation = request.get('operation', 'Unknown')
            request_groups[operation]['request_sum_work'].append(sum(durations))
            request_groups[operation]['request_max_wait'].append(max(durations))
            request_groups[operation]['segments_per_request'].append(len(request.get('segments', set())) or len(durations))

        for operation, data in sorted(request_groups.items()):
            sum_work = data['request_sum_work']
            max_wait = data['request_max_wait']
            request_s3_overhead[operation] = {
                'affected_requests': len(sum_work),
                'request_sum_work': self._calc_stats(sum_work),
                'request_max_wait': self._calc_stats(max_wait),
                'segments_per_request': self._calc_stats(data['segments_per_request']),
                'total_s3_work_ms': sum(sum_work),
                'total_estimated_request_wait_ms': sum(max_wait),
            }

        return {
            'total_load': self._calc_stats(total_load),
            'cache_miss_load': self._calc_stats(cache_miss_load),
            'substages': {
                stage: self._calc_stats(durs)
                for stage, durs in sorted(load_substages.items())
            },
            'total_load_by_operation': {
                op: self._calc_stats(durs)
                for op, durs in sorted(total_load_by_operation.items())
            },
            'cache_miss_by_request_operation': {
                op: self._calc_stats(durs)
                for op, durs in sorted(by_request_operation.items())
            },
            'request_s3_overhead': request_s3_overhead,
            'file_breakdown': self._build_file_breakdown(file_breakdown_events),
        }

    def _build_file_breakdown(self, events_by_stage: Dict[str, List[dict]]) -> Dict:
        """Summarize file/binlog-oriented metadata for LoadSegment substages."""
        breakdown: Dict[str, dict] = {}
        for stage, events in sorted(events_by_stage.items()):
            by_segment: Dict[str, dict] = defaultdict(lambda: {
                'event_count': 0,
                'total_duration_ms': 0.0,
                'fields': set(),
                'indexes': set(),
                'binlog_count': 0,
                'index_file_count': 0,
                'blob_count': 0,
                'field_binlog_count': 0,
            })

            total_binlogs = 0
            total_index_files = 0
            total_blobs = 0
            total_field_binlogs = 0
            for event in events:
                segment_id = str(event.get('segment_id', 'unknown'))
                segment = by_segment[segment_id]
                segment['event_count'] += 1
                segment['total_duration_ms'] += event.get('duration_ms', 0)

                if 'field_id' in event:
                    segment['fields'].add(event.get('field_id'))
                if 'index_id' in event:
                    segment['indexes'].add(event.get('index_id'))

                binlog_count = int(event.get('binlog_count', 0) or 0)
                index_file_count = int(event.get('index_file_count', 0) or 0)
                blob_count = int(event.get('blob_count', 0) or 0)
                field_binlog_count = int(event.get('field_binlog_count', 0) or 0)

                segment['binlog_count'] += binlog_count
                segment['index_file_count'] += index_file_count
                segment['blob_count'] += blob_count
                segment['field_binlog_count'] += field_binlog_count

                total_binlogs += binlog_count
                total_index_files += index_file_count
                total_blobs += blob_count
                total_field_binlogs += field_binlog_count

            segment_rows = []
            for segment_id, segment in by_segment.items():
                segment_rows.append({
                    'segment_id': segment_id,
                    'event_count': segment['event_count'],
                    'total_duration_ms': segment['total_duration_ms'],
                    'field_count': len(segment['fields']),
                    'index_count': len(segment['indexes']),
                    'binlog_count': segment['binlog_count'],
                    'index_file_count': segment['index_file_count'],
                    'blob_count': segment['blob_count'],
                    'field_binlog_count': segment['field_binlog_count'],
                })
            segment_rows.sort(key=lambda row: row['total_duration_ms'], reverse=True)

            breakdown[stage] = {
                'event_count': len(events),
                'segment_count': len(by_segment),
                'total_binlog_count': total_binlogs,
                'total_index_file_count': total_index_files,
                'total_blob_count': total_blobs,
                'total_field_binlog_count': total_field_binlogs,
                'top_segments': segment_rows[:5],
            }
        return breakdown

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
        self._print_load_operation_stats(load_stats)

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
                print(f"{operation} lazy DiskCache hit/miss events: {hits} / {misses}")
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

        cache_wait_latency = stats.get('cache_wait_latency', {})
        cache_wait_by_result = stats.get('cache_wait_by_result', {})
        request_overhead = stats.get('cache_wait_request_overhead', {})
        if cache_wait_latency:
            print("\nLazy-load cache wait before segment execution:")
            for operation in ('Search', 'Query'):
                op_stats = cache_wait_latency.get(operation, {})
                if op_stats.get('count', 0) <= 0:
                    continue
                self._print_stats_dict(op_stats, f"{operation} segment_cache_wait", indent="  ")

                by_result = cache_wait_by_result.get(operation, {})
                for result in ('cache_miss', 'cache_hit', 'error'):
                    result_stats = by_result.get(result, {})
                    if result_stats.get('count', 0) > 0:
                        label = result
                        if result == 'cache_miss':
                            label = 'physical cache-miss loader'
                        elif result == 'cache_hit':
                            label = 'non-loader lazy access'
                        self._print_stats_dict(result_stats, f"{operation} wait on {label}", indent="    ")

                req_stats = request_overhead.get(operation, {})
                if req_stats:
                    total_requests = stats.get('search_requests' if operation == 'Search' else 'query_requests', 0)
                    print(
                        f"  {operation} requests with lazy segment access: "
                        f"{self._format_count_pct(req_stats.get('requests_with_cache_wait', 0), total_requests)}"
                    )
                    print(
                        f"  {operation} requests that performed the physical miss load: "
                        f"{self._format_count_pct(req_stats.get('requests_with_miss_wait', 0), total_requests)}"
                    )
                    print(
                        f"  {operation} requests with non-loader lazy access: "
                        f"{self._format_count_pct(req_stats.get('requests_with_hit_wait', 0), total_requests)}"
                    )
                    print(
                        f"  {operation} requests with max cache wait > 10ms: "
                        f"{self._format_count_pct(req_stats.get('requests_wait_gt_10ms', 0), total_requests)}"
                    )
                    print(
                        f"  {operation} requests with max cache wait > 100ms: "
                        f"{self._format_count_pct(req_stats.get('requests_wait_gt_100ms', 0), total_requests)}"
                    )
                    print(
                        f"  {operation} requests with max cache wait > 1000ms: "
                        f"{self._format_count_pct(req_stats.get('requests_wait_gt_1000ms', 0), total_requests)}"
                    )
                    self._print_stats_dict(
                        req_stats.get('per_request_max_wait', {}),
                        f"{operation} per-request max cache wait",
                        indent="    ",
                    )
                    self._print_stats_dict(
                        req_stats.get('per_request_sum_wait', {}),
                        f"{operation} per-request sum cache wait",
                        indent="    ",
                    )
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

        total_by_op = stats.get('total_load_by_operation', {})
        for operation in ('LoadCollection', 'LoadPartition', 'Search', 'Query', 'Unknown'):
            op_stats = total_by_op.get(operation, {})
            if op_stats.get('count', 0) > 0:
                self._print_stats_dict(op_stats, f"LoadSegment total load during {operation}", indent="  ")

        request_overhead = stats.get('request_s3_overhead', {})
        if request_overhead:
            print("\nPhysical S3/object-store load grouped by initiating request:")
            for operation in ('Search', 'Query', 'Unknown'):
                op_stats = request_overhead.get(operation)
                if not op_stats:
                    continue
                print(f"  {operation}:")
                print(f"    Requests that performed physical load: {op_stats.get('affected_requests', 0)}")
                print(f"    Total physical S3 load work: {op_stats.get('total_s3_work_ms', 0):.2f} ms")
                print(
                    "    Estimated initiating-request wait: "
                    f"{op_stats.get('total_estimated_request_wait_ms', 0):.2f} ms "
                    "(sum of per-request max segment load)"
                )
                self._print_stats_dict(
                    op_stats.get('segments_per_request', {}),
                    "Segments physically loaded per initiating request",
                    indent="    ",
                )
                self._print_stats_dict(
                    op_stats.get('request_sum_work', {}),
                    "Per-request S3 load work, sum of segments",
                    indent="    ",
                )
                self._print_stats_dict(
                    op_stats.get('request_max_wait', {}),
                    "Per-initiating-request estimated wait, max segment load",
                    indent="    ",
                )

        substages = stats.get('substages', {})
        if substages:
            print("\nLoad substages:")
            for stage, stage_stats in substages.items():
                self._print_stats_dict(stage_stats, stage, indent="  ")

        file_breakdown = stats.get('file_breakdown', {})
        if file_breakdown:
            print("\nLoad file/binlog breakdown:")
            for stage, stage_stats in file_breakdown.items():
                print(f"  {stage}:")
                print(f"    Events:   {stage_stats.get('event_count', 0)}")
                print(f"    Segments: {stage_stats.get('segment_count', 0)}")
                if stage_stats.get('total_binlog_count', 0):
                    print(f"    Total binlogs: {stage_stats['total_binlog_count']}")
                if stage_stats.get('total_index_file_count', 0):
                    print(f"    Total index files: {stage_stats['total_index_file_count']}")
                if stage_stats.get('total_blob_count', 0):
                    print(f"    Total blobs: {stage_stats['total_blob_count']}")
                if stage_stats.get('total_field_binlog_count', 0):
                    print(f"    Total field binlogs: {stage_stats['total_field_binlog_count']}")
                top_segments = stage_stats.get('top_segments', [])
                if top_segments:
                    print("    Top segments by substage duration:")
                    for segment in top_segments:
                        details = [
                            f"segment={segment['segment_id']}",
                            f"events={segment['event_count']}",
                            f"total_ms={segment['total_duration_ms']:.2f}",
                        ]
                        if segment.get('field_count'):
                            details.append(f"fields={segment['field_count']}")
                        if segment.get('index_count'):
                            details.append(f"indexes={segment['index_count']}")
                        if segment.get('binlog_count'):
                            details.append(f"binlogs={segment['binlog_count']}")
                        if segment.get('index_file_count'):
                            details.append(f"index_files={segment['index_file_count']}")
                        if segment.get('blob_count'):
                            details.append(f"blobs={segment['blob_count']}")
                        if segment.get('field_binlog_count'):
                            details.append(f"field_binlogs={segment['field_binlog_count']}")
                        print(f"      - {', '.join(details)}")

    def _print_load_operation_stats(self, stats: Dict):
        stages = stats.get('stages', {})
        if not stages:
            print("LoadCollection/LoadPartition: No data")
            return

        for name, data in stages.items():
            self._print_stats_dict(data, name)

        status_counts = stats.get('status_counts', {})
        if status_counts:
            print("\nLoad lifecycle event counts:")
            for name, count in status_counts.items():
                print(f"  {name}: {count}")

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
            request_overhead = seg_stats.get('request_s3_overhead', {})
            for operation in ('Search', 'Query'):
                op_stats = request_overhead.get(operation)
                if not op_stats:
                    continue
                max_wait = op_stats.get('request_max_wait', {})
                if max_wait.get('count', 0) > 0:
                    print(
                        f"   - {operation} requests that performed physical S3 load: "
                        f"{op_stats.get('affected_requests', 0)}"
                    )
                    print(
                        f"   - Initiating {operation} wait mean/P95: "
                        f"{max_wait.get('mean', 0):.2f} / {max_wait.get('p95', 0):.2f} ms"
                    )
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
            cache_wait_overhead = search_stats.get('cache_wait_request_overhead', {})
            for operation in ('Search', 'Query'):
                op_stats = cache_wait_overhead.get(operation)
                if not op_stats:
                    continue
                max_wait = op_stats.get('per_request_max_wait', {})
                if max_wait.get('count', 0) > 0:
                    total_op_requests = search_stats.get('search_requests' if operation == 'Search' else 'query_requests', 0)
                    print(
                        f"   - {operation} requests with lazy segment cache wait: "
                        f"{self._format_count_pct(op_stats.get('requests_with_cache_wait', 0), total_op_requests)}"
                    )
                    print(
                        f"   - {operation} requests with cache wait >100ms/>1000ms: "
                        f"{self._format_count_pct(op_stats.get('requests_wait_gt_100ms', 0), total_op_requests)} / "
                        f"{self._format_count_pct(op_stats.get('requests_wait_gt_1000ms', 0), total_op_requests)}"
                    )
                    print(
                        f"   - {operation} request max cache wait mean/P95: "
                        f"{max_wait.get('mean', 0):.2f} / {max_wait.get('p95', 0):.2f} ms"
                    )

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
    def _format_count_pct(count: int, total: int) -> str:
        if total <= 0:
            return str(count)
        return f"{count} / {total} ({(count / total) * 100:.2f}%)"

    @staticmethod
    def _is_truthy(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'y')
        return False

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
