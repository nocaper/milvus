#!/usr/bin/env python3
"""
Milvus Query Trace Analysis Script - 清晰版

专注于 Query 请求的时延分解分析，按路径分类统计请求个数和时延。

三种 Query 路径分类：
  1. 热查询 (Hot Path): segment 已在内存，直接执行
  2. 冷查询 (Cold Path): 触发 lazy-load，从对象存储加载
  3. 等待查询 (Wait Path): 等待其他并发请求加载

使用方法:
    python analyze_query_traces.py milvus.log
    python analyze_query_traces.py milvus.log --output ./query_report
    python analyze_query_traces.py milvus.log --format json
    python analyze_query_traces.py milvus.log --unknown-output ./unknown_requests.log
"""

import re
import sys
import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Warning: pandas not found, CSV export will be disabled", file=sys.stderr)

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_PLOT = True
    sns.set_theme(style="whitegrid")
except ImportError:
    HAS_PLOT = False
    print("Warning: matplotlib/seaborn not found, chart generation will be disabled", file=sys.stderr)


# ============================================================================
# 数据结构定义
# ============================================================================

class QueryPath(Enum):
    """Query 请求的三种路径分类"""
    HOT = "hot"          # 热查询：segment 已在内存
    COLD = "cold"        # 冷查询：触发物理加载
    WAIT = "wait"        # 等待查询：等待其他请求加载
    UNKNOWN = "unknown"  # 无法分类


@dataclass
class QueryStageLatency:
    """单个阶段的延迟统计"""
    count: int = 0
    total_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    avg_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0

    def calculate_percentiles(self, durations: List[float]):
        """计算百分位数"""
        if not durations:
            return
        sorted_durs = sorted(durations)
        self.count = len(sorted_durs)
        self.total_ms = sum(sorted_durs)
        self.min_ms = sorted_durs[0]
        self.max_ms = sorted_durs[-1]
        self.avg_ms = self.total_ms / self.count
        self.p50_ms = self._percentile(sorted_durs, 50)
        self.p95_ms = self._percentile(sorted_durs, 95)
        self.p99_ms = self._percentile(sorted_durs, 99)

    @staticmethod
    def _percentile(sorted_list: List[float], p: int) -> float:
        """计算第 p 百分位"""
        if not sorted_list:
            return 0.0
        k = (len(sorted_list) - 1) * p / 100
        f = int(k)
        c = f + 1
        if c >= len(sorted_list):
            return sorted_list[-1]
        return sorted_list[f] + (k - f) * (sorted_list[c] - sorted_list[f])


@dataclass
class QueryRequest:
    """单个 Query 请求的完整信息"""
    trace_id: str
    path: QueryPath
    operation: str = "Query"

    # 各阶段延迟
    route_ms: float = 0.0

    # Segment 统计
    sealed_count: int = 0
    growing_count: int = 0

    # Cache 相关
    cache_wait_segments: List[Dict] = None  # 每个 segment 的等待详情
    max_cache_wait_ms: float = 0.0          # 请求可见的最大等待
    total_cache_wait_ms: float = 0.0        # 总等待工作量

    # 执行相关
    segment_query_durations: List[float] = None  # 每个 segment 的执行时间
    max_segment_query_ms: float = 0.0
    total_segment_query_ms: float = 0.0

    # LoadSegment 相关（仅 cold path）
    load_segment_count: int = 0
    load_segment_durations: List[float] = None
    max_load_ms: float = 0.0
    total_load_ms: float = 0.0

    # 总延迟（估算）
    estimated_total_ms: float = 0.0

    def __post_init__(self):
        if self.cache_wait_segments is None:
            self.cache_wait_segments = []
        if self.segment_query_durations is None:
            self.segment_query_durations = []
        if self.load_segment_durations is None:
            self.load_segment_durations = []

    def calculate_totals(self):
        """计算汇总指标"""
        # Cache wait
        if self.cache_wait_segments:
            wait_durs = [s['duration_ms'] for s in self.cache_wait_segments]
            self.max_cache_wait_ms = max(wait_durs) if wait_durs else 0.0
            self.total_cache_wait_ms = sum(wait_durs)

        # Segment query
        if self.segment_query_durations:
            self.max_segment_query_ms = max(self.segment_query_durations)
            self.total_segment_query_ms = sum(self.segment_query_durations)

        # Load segment
        if self.load_segment_durations:
            self.load_segment_count = len(self.load_segment_durations)
            self.max_load_ms = max(self.load_segment_durations)
            self.total_load_ms = sum(self.load_segment_durations)

        # 估算总延迟（route + max(cache_wait, load) + max(segment_query)）
        self.estimated_total_ms = (
            self.route_ms +
            max(self.max_cache_wait_ms, self.max_load_ms) +
            self.max_segment_query_ms
        )


# ============================================================================
# 日志解析
# ============================================================================

_TRACE_RE = re.compile(
    r'\[LATENCY_TRACE\]\s+'
    r'trace_id=(\S*)\s+'
    r'operation=(\S+)\s+'
    r'stage=(\S+)\s+'
    r'component=(\S+)\s+'
    r'duration_ms=([\d.]+)'
    r'(.*)'
)


def parse_trace_line(line: str) -> Optional[dict]:
    """解析单行 trace 日志"""
    m = _TRACE_RE.search(line)
    if not m:
        return None

    event = {
        'trace_id': m.group(1),
        'operation': m.group(2),
        'stage': m.group(3),
        'component': m.group(4),
        'duration_ms': float(m.group(5)),
    }

    # 解析 metadata
    metadata_text = m.group(6).strip()
    if metadata_text:
        metadata_re = re.compile(r'(\w+)=((?:\[[^\]]*\])|"[^"]*"|\S+)')
        for match in metadata_re.finditer(metadata_text):
            key = match.group(1)
            value = match.group(2).strip('"')
            try:
                event[key] = int(value)
            except ValueError:
                try:
                    event[key] = float(value)
                except ValueError:
                    # 布尔值处理
                    if value.lower() in ('true', 'false'):
                        event[key] = value.lower() == 'true'
                    else:
                        event[key] = value

    return event


# Both read operations use the same route/cache path shape. Search differs
# only in the hot segment execution stage: segment_search vs segment_query.
READ_OPERATIONS = ("Search", "Query")


# ============================================================================
# Query 分析器
# ============================================================================

class QueryAnalyzer:
    """Query 请求分析器"""

    def __init__(self):
        self.events: List[dict] = []
        self.events_by_trace: Dict[str, List[dict]] = defaultdict(list)
        self.query_requests: Dict[str, QueryRequest] = {}
        self.operation_trace_ids: Dict[str, set] = defaultdict(set)
        self.operation_counts: Dict[str, int] = defaultdict(int)

        # 统计信息
        self.total_lines = 0
        self.parsed_lines = 0
        self.query_traces = 0

    def load_from_file(self, filepath: str):
        """从文件加载 trace 日志"""
        print(f"Loading trace logs from: {filepath}")

        if filepath == '-':
            source = sys.stdin
        else:
            source = open(filepath, 'r', encoding='utf-8', errors='replace')

        try:
            for line in source:
                if '[LATENCY_TRACE]' not in line:
                    continue

                self.total_lines += 1
                event = parse_trace_line(line)

                if event:
                    self.parsed_lines += 1
                    self.events.append(event)

                    # Keep only Search/Query operations for request analysis.
                    self.operation_counts[event['operation']] += 1
                    if event['operation'] in READ_OPERATIONS:
                        trace_id = event['trace_id']
                        self.events_by_trace[trace_id].append(event)
                        self.operation_trace_ids[event['operation']].add(trace_id)
        finally:
            if filepath != '-':
                source.close()

        self.query_traces = len(self.events_by_trace)
        print(f"✓ Loaded {self.parsed_lines} trace events from {self.total_lines} lines")
        print(f"✓ Found {self.query_traces} Search/Query requests")
        for operation in READ_OPERATIONS:
            count = len(self.operation_trace_ids.get(operation, set()))
            if count:
                print(f"  {operation} requests: {count}")
        if not self.query_traces and self.operation_counts:
            operations = ", ".join(
                f"{name}={count}" for name, count in sorted(self.operation_counts.items())
            )
            print(f"  Parsed operations: {operations}")

    def analyze(self):
        """Analyze all Search/Query requests."""
        print("\nAnalyzing Search/Query requests...")

        for trace_id, events in self.events_by_trace.items():
            request = self._analyze_single_request(trace_id, events)
            self.query_requests[trace_id] = request

        print(f"✓ Analyzed {len(self.query_requests)} Search/Query requests")

    def export_unknown_requests(self, filepath: str):
        """Write UNKNOWN requests and their parsed events as readable text."""
        unknown_requests = [
            (trace_id, request)
            for trace_id, request in self.query_requests.items()
            if request.path == QueryPath.UNKNOWN
        ]

        output_path = Path(filepath)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open('w', encoding='utf-8') as output:
            output.write("UNKNOWN Search/Query requests\n")
            output.write("=" * 100 + "\n")
            output.write(f"total_unknown_requests={len(unknown_requests)}\n\n")

            for index, (trace_id, request) in enumerate(unknown_requests, start=1):
                events = self.events_by_trace.get(trace_id, [])
                stages = sorted({event.get('stage', '') for event in events})
                output.write(f"UNKNOWN_REQUEST {index}\n")
                output.write("-" * 100 + "\n")
                output.write(f"trace_id={trace_id}\n")
                output.write(f"operation={request.operation}\n")
                output.write(f"event_count={len(events)}\n")
                output.write(f"stages={','.join(stages)}\n")
                output.write(f"sealed_count={request.sealed_count}\n")
                output.write(f"growing_count={request.growing_count}\n")
                output.write("reason=no segment execution or cache-wait event was associated with this trace_id\n")
                output.write("events:\n")

                for event in events:
                    fixed_fields = (
                        'trace_id',
                        'operation',
                        'stage',
                        'component',
                        'duration_ms',
                    )
                    fields = [
                        f"{key}={event[key]}"
                        for key in fixed_fields
                        if key in event
                    ]
                    metadata = [
                        f"{key}={event[key]}"
                        for key in sorted(event)
                        if key not in fixed_fields
                    ]
                    output.write("  [LATENCY_TRACE] " + " ".join(fields + metadata) + "\n")

                output.write("\n")

        print(f"✓ Wrote {len(unknown_requests)} UNKNOWN requests to {output_path}")

    def _analyze_single_request(self, trace_id: str, events: List[dict]) -> QueryRequest:
        """Analyze one Search/Query request."""
        operation = events[0].get('operation', 'Unknown') if events else 'Unknown'
        request = QueryRequest(
            trace_id=trace_id,
            path=QueryPath.UNKNOWN,
            operation=operation,
        )

        # 用于判断路径类型
        has_cache_miss = False
        has_waited_for_load = False
        has_cache_wait = False

        for event in events:
            stage = event['stage']
            duration = event['duration_ms']

            # Route 阶段
            if stage == 'route':
                request.route_ms = duration

            # Segment 统计
            elif stage == 'segment_stats':
                request.sealed_count = event.get('sealed_segments', 0)
                request.growing_count = event.get('growing_segments', 0)

            # Cache wait 分析
            elif stage == 'segment_cache_wait':
                has_cache_wait = True
                cache_miss = event.get('cache_miss', False)
                waited_for_load = event.get('waited_for_load', False)

                if cache_miss:
                    has_cache_miss = True
                elif waited_for_load:
                    has_waited_for_load = True

                request.cache_wait_segments.append({
                    'segment_id': event.get('segment_id', 'unknown'),
                    'duration_ms': duration,
                    'cache_miss': cache_miss,
                    'waited_for_load': waited_for_load,
                    'source': event.get('source', 'unknown'),
                })

            # Search and Query use separate hot execution stage names.
            elif stage in ('segment_search', 'segment_query'):
                request.segment_query_durations.append(duration)

            # LoadSegment（只在 cold path 出现）
            elif stage == 'segment_cache_load':
                request.load_segment_durations.append(duration)

        # 判断路径类型
        if has_cache_miss:
            request.path = QueryPath.COLD
        elif has_waited_for_load:
            request.path = QueryPath.WAIT
        elif has_cache_wait or request.segment_query_durations:
            # 有 cache_wait 但没有 miss/wait，或者直接有 segment_query
            request.path = QueryPath.HOT
        else:
            request.path = QueryPath.UNKNOWN

        # 计算汇总指标
        request.calculate_totals()

        return request

    def generate_summary(self) -> Dict:
        """生成汇总统计"""
        complete_requests = [
            request
            for request in self.query_requests.values()
            if request.path != QueryPath.UNKNOWN
        ]
        incomplete_requests = len(self.query_requests) - len(complete_requests)

        summary = {
            # Path statistics use only requests with a known path. Route-only
            # requests remain observable through incomplete_requests.
            'total_requests': len(complete_requests),
            'observed_requests': len(self.query_requests),
            'incomplete_requests': incomplete_requests,
            'by_path': {},
            'overall_stages': {},
        }

        # 按路径分类
        requests_by_path = defaultdict(list)
        for req in complete_requests:
            requests_by_path[req.path].append(req)

        for path, requests in requests_by_path.items():
            path_stats = self._calculate_path_statistics(requests)
            summary['by_path'][path.value] = path_stats

        # 整体阶段统计
        summary['overall_stages'] = self._calculate_overall_stages()

        return summary

    def _calculate_path_statistics(self, requests: List[QueryRequest]) -> Dict:
        """计算特定路径的统计信息"""
        if not requests:
            return {
                'count': 0,
                'percentage': 0.0,
            }

        count = len(requests)
        complete_request_count = sum(
            1
            for request in self.query_requests.values()
            if request.path != QueryPath.UNKNOWN
        )
        percentage = (count / complete_request_count) * 100 if complete_request_count else 0.0

        # 收集各阶段的延迟
        route_durs = [r.route_ms for r in requests if r.route_ms > 0]
        cache_wait_max_durs = [r.max_cache_wait_ms for r in requests if r.max_cache_wait_ms > 0]
        cache_wait_total_durs = [r.total_cache_wait_ms for r in requests if r.total_cache_wait_ms > 0]
        segment_query_max_durs = [r.max_segment_query_ms for r in requests if r.max_segment_query_ms > 0]
        segment_query_total_durs = [r.total_segment_query_ms for r in requests if r.total_segment_query_ms > 0]
        load_max_durs = [r.max_load_ms for r in requests if r.max_load_ms > 0]
        load_total_durs = [r.total_load_ms for r in requests if r.total_load_ms > 0]
        estimated_total_durs = [r.estimated_total_ms for r in requests if r.estimated_total_ms > 0]

        # Segment 统计
        sealed_counts = [r.sealed_count for r in requests if r.sealed_count > 0]
        growing_counts = [r.growing_count for r in requests if r.growing_count > 0]

        stats = {
            'count': count,
            'percentage': round(percentage, 2),
            'route': self._make_latency_stats(route_durs),
            'cache_wait_max': self._make_latency_stats(cache_wait_max_durs),
            'cache_wait_total': self._make_latency_stats(cache_wait_total_durs),
            'segment_query_max': self._make_latency_stats(segment_query_max_durs),
            'segment_query_total': self._make_latency_stats(segment_query_total_durs),
            'estimated_total': self._make_latency_stats(estimated_total_durs),
            'segments': {
                'sealed_avg': round(sum(sealed_counts) / len(sealed_counts), 2) if sealed_counts else 0.0,
                'growing_avg': round(sum(growing_counts) / len(growing_counts), 2) if growing_counts else 0.0,
            }
        }

        # Cold path 特有的统计
        if load_max_durs:
            stats['load_segment_max'] = self._make_latency_stats(load_max_durs)
            stats['load_segment_total'] = self._make_latency_stats(load_total_durs)
            stats['load_count_avg'] = round(
                sum(r.load_segment_count for r in requests) / len(requests), 2
            )

        return stats

    def _make_latency_stats(self, durations: List[float]) -> Dict:
        """生成延迟统计字典"""
        if not durations:
            return {
                'count': 0,
                'min': 0.0,
                'max': 0.0,
                'avg': 0.0,
                'p50': 0.0,
                'p95': 0.0,
                'p99': 0.0,
            }

        latency = QueryStageLatency()
        latency.calculate_percentiles(durations)

        return {
            'count': latency.count,
            'min': round(latency.min_ms, 2),
            'max': round(latency.max_ms, 2),
            'avg': round(latency.avg_ms, 2),
            'p50': round(latency.p50_ms, 2),
            'p95': round(latency.p95_ms, 2),
            'p99': round(latency.p99_ms, 2),
        }

    def _calculate_overall_stages(self) -> Dict:
        """计算所有请求的各阶段统计（不分路径）"""
        all_requests = [
            request
            for request in self.query_requests.values()
            if request.path != QueryPath.UNKNOWN
        ]

        return {
            'route': self._make_latency_stats([r.route_ms for r in all_requests if r.route_ms > 0]),
            'cache_wait_max': self._make_latency_stats([r.max_cache_wait_ms for r in all_requests if r.max_cache_wait_ms > 0]),
            'segment_query_max': self._make_latency_stats([r.max_segment_query_ms for r in all_requests if r.max_segment_query_ms > 0]),
            'estimated_total': self._make_latency_stats([r.estimated_total_ms for r in all_requests if r.estimated_total_ms > 0]),
        }


# ============================================================================
# 报告生成
# ============================================================================

class ReportGenerator:
    """报告生成器"""

    def __init__(self, analyzer: QueryAnalyzer, output_dir: Optional[str] = None):
        self.analyzer = analyzer
        self.output_dir = Path(output_dir) if output_dir else None

        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_console_report(self, summary: Dict):
        """生成控制台报告"""
        print("\n" + "=" * 80)
        print("MILVUS SEARCH/QUERY TRACE ANALYSIS REPORT")
        print("=" * 80)

        total = summary['total_requests']
        observed = summary.get('observed_requests', total)
        incomplete = summary.get('incomplete_requests', 0)
        print(f"\n📊 Complete Search/Query Requests: {total}")
        print(f"   Observed requests: {observed}")
        print(f"   Excluded incomplete requests: {incomplete}")
        print("   Path percentages use complete requests only")

        if total == 0:
            if incomplete:
                print("\n⚠️  No complete Search/Query paths found in the trace log")
            else:
                print("\n⚠️  No Search/Query requests found in the trace log")
            return

        print("\n" + "-" * 80)
        print("Query Path Distribution")
        print("-" * 80)

        # 路径分布汇总
        by_path = summary['by_path']

        print(f"\n{'Path':<15} {'Count':<10} {'Percentage':<12}")
        print("-" * 40)
        for path_name in ['hot', 'cold', 'wait', 'unknown']:
            if path_name in by_path:
                stats = by_path[path_name]
                print(f"{path_name.upper():<15} {stats['count']:<10} {stats['percentage']:>6.2f}%")

        # 各路径详细统计
        for path_name in ['hot', 'cold', 'wait']:
            if path_name not in by_path:
                continue

            stats = by_path[path_name]
            if stats['count'] == 0:
                continue

            print("\n" + "=" * 80)
            print(f"{path_name.upper()} PATH: {stats['count']} requests ({stats['percentage']:.2f}%)")
            print("=" * 80)

            # Route
            if stats['route']['count'] > 0:
                print(f"\n📍 Route Stage:")
                self._print_latency_stats(stats['route'])

            # Cache Wait
            if stats['cache_wait_max']['count'] > 0:
                print(f"\n⏱️  Cache Wait (Max per request):")
                self._print_latency_stats(stats['cache_wait_max'])

                if path_name == 'cold':
                    print(f"\n   💡 Note: In COLD path, cache_wait includes load time")
                elif path_name == 'wait':
                    print(f"\n   💡 Note: In WAIT path, cache_wait is time waiting for other loaders")

            # Load Segment (only for cold path)
            if path_name == 'cold' and 'load_segment_max' in stats:
                print(f"\n📦 Load Segment (Max per request):")
                self._print_latency_stats(stats['load_segment_max'])
                print(f"\n   Average segments loaded per request: {stats.get('load_count_avg', 0):.2f}")

            # Segment Query
            if stats['segment_query_max']['count'] > 0:
                print(f"\n🔍 Segment Query Execution (Max per request):")
                self._print_latency_stats(stats['segment_query_max'])

            # Estimated Total
            if stats['estimated_total']['count'] > 0:
                print(f"\n⏰ Estimated Total Latency (route + max(cache/load) + max(query)):")
                self._print_latency_stats(stats['estimated_total'])

            # Segment counts
            print(f"\n📊 Average Segments per Request:")
            print(f"   Sealed:  {stats['segments']['sealed_avg']:.2f}")
            print(f"   Growing: {stats['segments']['growing_avg']:.2f}")

        # 整体统计
        print("\n" + "=" * 80)
        print("OVERALL STAGE STATISTICS (All Paths)")
        print("=" * 80)

        overall = summary['overall_stages']

        if overall['route']['count'] > 0:
            print(f"\n📍 Route:")
            self._print_latency_stats(overall['route'])

        if overall['cache_wait_max']['count'] > 0:
            print(f"\n⏱️  Cache Wait (Max):")
            self._print_latency_stats(overall['cache_wait_max'])

        if overall['segment_query_max']['count'] > 0:
            print(f"\n🔍 Segment Query (Max):")
            self._print_latency_stats(overall['segment_query_max'])

        if overall['estimated_total']['count'] > 0:
            print(f"\n⏰ Estimated Total:")
            self._print_latency_stats(overall['estimated_total'])

        print("\n" + "=" * 80)
        print("KEY INSIGHTS")
        print("=" * 80)
        self._print_insights(summary)

        print("\n")

    def _print_latency_stats(self, stats: Dict):
        """打印延迟统计"""
        print(f"   Count: {stats['count']}")
        print(f"   Min:   {stats['min']:>8.2f} ms")
        print(f"   Avg:   {stats['avg']:>8.2f} ms")
        print(f"   P50:   {stats['p50']:>8.2f} ms")
        print(f"   P95:   {stats['p95']:>8.2f} ms")
        print(f"   P99:   {stats['p99']:>8.2f} ms")
        print(f"   Max:   {stats['max']:>8.2f} ms")

    def _print_insights(self, summary: Dict):
        """打印关键洞察"""
        total = summary['total_requests']
        by_path = summary['by_path']

        insights = []

        # Hot path 占比
        if 'hot' in by_path:
            hot_pct = by_path['hot']['percentage']
            if hot_pct > 80:
                insights.append(f"✓ {hot_pct:.1f}% requests are HOT path - good cache hit rate!")
            elif hot_pct < 20:
                insights.append(f"⚠ Only {hot_pct:.1f}% requests are HOT path - consider preloading")

        # Cold path 影响
        if 'cold' in by_path and by_path['cold']['count'] > 0:
            cold_stats = by_path['cold']
            cold_pct = cold_stats['percentage']
            if 'load_segment_max' in cold_stats:
                cold_p95 = cold_stats['load_segment_max']['p95']
                insights.append(f"⚠ {cold_pct:.1f}% requests trigger lazy-load (P95: {cold_p95:.2f}ms)")

        # Wait path 影响
        if 'wait' in by_path and by_path['wait']['count'] > 0:
            wait_stats = by_path['wait']
            wait_pct = wait_stats['percentage']
            wait_p95 = wait_stats['cache_wait_max']['p95']
            insights.append(f"⚠ {wait_pct:.1f}% requests wait for concurrent loaders (P95: {wait_p95:.2f}ms)")

        # Segment query 性能
        overall = summary['overall_stages']
        if overall['segment_query_max']['count'] > 0:
            query_p95 = overall['segment_query_max']['p95']
            insights.append(f"ℹ Segment query execution P95: {query_p95:.2f}ms")

        if insights:
            for insight in insights:
                print(f"\n{insight}")
        else:
            print("\nNo specific insights available.")

    def export_csv(self, summary: Dict):
        """导出 CSV 报告"""
        if not HAS_PANDAS or not self.output_dir:
            return

        print("\nExporting CSV reports...")

        # 1. Path summary
        path_data = []
        for path_name, stats in summary['by_path'].items():
            path_data.append({
                'path': path_name,
                'count': stats['count'],
                'percentage': stats['percentage'],
                'route_avg': stats['route']['avg'],
                'route_p95': stats['route']['p95'],
                'cache_wait_avg': stats['cache_wait_max']['avg'],
                'cache_wait_p95': stats['cache_wait_max']['p95'],
                'segment_query_avg': stats['segment_query_max']['avg'],
                'segment_query_p95': stats['segment_query_max']['p95'],
                'estimated_total_avg': stats['estimated_total']['avg'],
                'estimated_total_p95': stats['estimated_total']['p95'],
            })

        df_path = pd.DataFrame(path_data)
        path_file = self.output_dir / 'query_path_summary.csv'
        df_path.to_csv(path_file, index=False)
        print(f"  ✓ {path_file}")

        # 2. Detailed requests
        request_data = []
        for req in self.analyzer.query_requests.values():
            request_data.append({
                'trace_id': req.trace_id,
                'operation': req.operation,
                'path': req.path.value,
                'route_ms': req.route_ms,
                'sealed_count': req.sealed_count,
                'growing_count': req.growing_count,
                'max_cache_wait_ms': req.max_cache_wait_ms,
                'total_cache_wait_ms': req.total_cache_wait_ms,
                'max_segment_query_ms': req.max_segment_query_ms,
                'total_segment_query_ms': req.total_segment_query_ms,
                'load_segment_count': req.load_segment_count,
                'max_load_ms': req.max_load_ms,
                'total_load_ms': req.total_load_ms,
                'estimated_total_ms': req.estimated_total_ms,
            })

        df_requests = pd.DataFrame(request_data)
        requests_file = self.output_dir / 'query_requests_detail.csv'
        df_requests.to_csv(requests_file, index=False)
        print(f"  ✓ {requests_file}")

    def export_json(self, summary: Dict):
        """导出 JSON 报告"""
        if not self.output_dir:
            return

        print("\nExporting JSON report...")

        json_file = self.output_dir / 'query_analysis.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        print(f"  ✓ {json_file}")

    def generate_charts(self, summary: Dict):
        """生成图表"""
        if not HAS_PLOT or not self.output_dir:
            return

        print("\nGenerating charts...")

        # 1. Path distribution pie chart
        self._plot_path_distribution(summary)

        # 2. Latency comparison by path
        self._plot_latency_by_path(summary)

        # 3. Latency breakdown for each path
        self._plot_latency_breakdown(summary)

    def _plot_path_distribution(self, summary: Dict):
        """绘制路径分布饼图"""
        by_path = summary['by_path']

        labels = []
        sizes = []
        colors = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3']

        path_names = {'hot': 'Hot Path', 'cold': 'Cold Path', 'wait': 'Wait Path', 'unknown': 'Unknown'}

        for i, (path_key, path_label) in enumerate(path_names.items()):
            if path_key in by_path and by_path[path_key]['count'] > 0:
                labels.append(f"{path_label}\n{by_path[path_key]['count']} ({by_path[path_key]['percentage']:.1f}%)")
                sizes.append(by_path[path_key]['count'])

        if not sizes:
            return

        fig, ax = plt.subplots(figsize=(10, 7))
        ax.pie(sizes, labels=labels, colors=colors[:len(sizes)], autopct='%1.1f%%', startangle=90)
        ax.set_title('Query Path Distribution', fontsize=16, fontweight='bold')

        output_file = self.output_dir / 'query_path_distribution.png'
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"  ✓ {output_file}")

    def _plot_latency_by_path(self, summary: Dict):
        """绘制各路径的延迟对比"""
        by_path = summary['by_path']

        paths = []
        route_p95 = []
        cache_wait_p95 = []
        query_p95 = []
        total_p95 = []

        for path_key in ['hot', 'cold', 'wait']:
            if path_key not in by_path or by_path[path_key]['count'] == 0:
                continue

            stats = by_path[path_key]
            paths.append(path_key.upper())
            route_p95.append(stats['route']['p95'])
            cache_wait_p95.append(stats['cache_wait_max']['p95'])
            query_p95.append(stats['segment_query_max']['p95'])
            total_p95.append(stats['estimated_total']['p95'])

        if not paths:
            return

        x = range(len(paths))
        width = 0.2

        fig, ax = plt.subplots(figsize=(12, 7))

        ax.bar([i - 1.5*width for i in x], route_p95, width, label='Route', color='#66c2a5')
        ax.bar([i - 0.5*width for i in x], cache_wait_p95, width, label='Cache Wait', color='#fc8d62')
        ax.bar([i + 0.5*width for i in x], query_p95, width, label='Segment Query', color='#8da0cb')
        ax.bar([i + 1.5*width for i in x], total_p95, width, label='Estimated Total', color='#e78ac3')

        ax.set_xlabel('Query Path', fontsize=12)
        ax.set_ylabel('Latency (ms) - P95', fontsize=12)
        ax.set_title('Query Latency Comparison by Path (P95)', fontsize=16, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(paths)
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        output_file = self.output_dir / 'query_latency_by_path.png'
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"  ✓ {output_file}")

    def _plot_latency_breakdown(self, summary: Dict):
        """绘制各路径的延迟分解堆叠图"""
        by_path = summary['by_path']

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))

        path_configs = [
            ('hot', 'Hot Path', 0),
            ('cold', 'Cold Path', 1),
            ('wait', 'Wait Path', 2),
        ]

        for path_key, path_label, idx in path_configs:
            if path_key not in by_path or by_path[path_key]['count'] == 0:
                axes[idx].text(0.5, 0.5, f'No {path_label} data',
                              ha='center', va='center', transform=axes[idx].transAxes)
                axes[idx].set_title(path_label)
                continue

            stats = by_path[path_key]

            stages = ['Route', 'Cache Wait', 'Segment Query']
            avg_values = [
                stats['route']['avg'],
                stats['cache_wait_max']['avg'],
                stats['segment_query_max']['avg'],
            ]
            p95_values = [
                stats['route']['p95'],
                stats['cache_wait_max']['p95'],
                stats['segment_query_max']['p95'],
            ]

            x = ['Avg', 'P95']
            width = 0.6

            # 堆叠柱状图
            bottom_avg = 0
            bottom_p95 = 0
            colors = ['#66c2a5', '#fc8d62', '#8da0cb']

            for i, (stage, color) in enumerate(zip(stages, colors)):
                axes[idx].bar(['Avg'], [avg_values[i]], width, bottom=bottom_avg, label=stage, color=color)
                axes[idx].bar(['P95'], [p95_values[i]], width, bottom=bottom_p95, color=color)
                bottom_avg += avg_values[i]
                bottom_p95 += p95_values[i]

            axes[idx].set_ylabel('Latency (ms)')
            axes[idx].set_title(f'{path_label}\n({stats["count"]} requests)', fontweight='bold')
            axes[idx].legend()
            axes[idx].grid(axis='y', alpha=0.3)

        fig.suptitle('Query Latency Breakdown by Path', fontsize=16, fontweight='bold')

        output_file = self.output_dir / 'query_latency_breakdown.png'
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"  ✓ {output_file}")


# ============================================================================
# 主程序
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Analyze Milvus Query trace logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('logfile', help='Path to log file (or "-" for stdin)')
    parser.add_argument('-o', '--output', help='Output directory for reports')
    parser.add_argument('-f', '--format', choices=['console', 'csv', 'json', 'all'],
                       default='all', help='Output format (default: all)')
    parser.add_argument('--no-charts', action='store_true', help='Disable chart generation')
    parser.add_argument(
        '--unknown-output',
        help='Write UNKNOWN Search/Query requests and parsed events to a plain text file',
    )

    args = parser.parse_args()

    # 分析
    analyzer = QueryAnalyzer()
    analyzer.load_from_file(args.logfile)

    if analyzer.query_traces == 0:
        print("\n⚠️  No Search/Query requests found in the log file")
        if analyzer.operation_counts:
            operations = ", ".join(
                f"{name}={count}" for name, count in sorted(analyzer.operation_counts.items())
            )
            print(f"Parsed operations: {operations}")
            print(
                "This log may contain only non-read operations, or it may have "
                "been produced by a different tracer format."
            )
        return 1

    analyzer.analyze()
    summary = analyzer.generate_summary()

    if args.unknown_output:
        analyzer.export_unknown_requests(args.unknown_output)

    # 生成报告
    generator = ReportGenerator(analyzer, args.output)

    if args.format in ('console', 'all'):
        generator.generate_console_report(summary)

    if args.output:
        if args.format in ('csv', 'all'):
            generator.export_csv(summary)

        if args.format in ('json', 'all'):
            generator.export_json(summary)

        if not args.no_charts and args.format == 'all':
            generator.generate_charts(summary)

    return 0


if __name__ == '__main__':
    sys.exit(main())
