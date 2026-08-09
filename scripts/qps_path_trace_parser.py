#!/usr/bin/env python3
"""
Parse Milvus qps path profiling logs.

The parser accepts both Go zap JSON logs:
  {"msg":"milvus_qps_path_trace","op":"load_field_data",...}

and C++ text logs:
  milvus_qps_path_object_io op=remote_read_cpp path=... bytes=... latency_ms=...
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


TRACE_MARKER = "milvus_qps_path_trace"
OBJECT_IO_MARKER = "milvus_qps_path_object_io"
MARKERS = (TRACE_MARKER, OBJECT_IO_MARKER)

QUERYNODE_READ_SIDE_OPS = {
    "load_field_data",
    "load_multi_field_data",
    "load_bloom_filter",
    "load_delta_logs",
}

SEGMENT_LOAD_TOPLINE_OPS = {"load_segment"}
DATANODE_WRITE_SIDE_OPS = {"datanode_write_logs", "datanode_write_storage_v2"}

EVENT_CSV_FIELDS = [
    "source",
    "line_no",
    "event_type",
    "component",
    "op",
    "path_hit",
    "object_storage_path_hit",
    "path_kind",
    "status",
    "latency_ms",
    "event_bytes",
    "datanode_bytes",
    "object_io_bytes",
    "remote_bytes",
    "remote_file_count",
    "datanode_remote_file_count",
    "datanode_remote_log_bytes",
    "datanode_remote_mem_bytes",
    "datanode_entry_count",
    "insert_log_file_count",
    "insert_log_bytes",
    "stats_log_file_count",
    "stats_log_bytes",
    "delta_log_file_count",
    "delta_log_bytes",
    "index_file_count",
    "index_bytes",
    "storage_v2",
    "storage_version",
    "row_count",
    "collectionID",
    "partitionID",
    "segmentID",
    "fieldID",
    "indexID",
    "buildID",
    "channel",
    "scope",
    "nq",
    "topk",
    "segmentType",
    "segmentLevel",
    "skip_reason",
    "planned_path_kind",
    "planned_datanode_minio_querynode_path_hit",
    "planned_datanode_remote_file_count",
    "planned_datanode_remote_log_bytes",
    "planned_index_file_count",
    "planned_index_bytes",
    "bucket",
    "path",
    "actual_bytes",
    "error",
]


@dataclass
class Agg:
    count: int = 0
    success: int = 0
    fail: int = 0
    latency_sum_ms: float = 0.0
    latencies_ms: List[float] = field(default_factory=list)
    event_bytes_sum: int = 0
    datanode_bytes_sum: int = 0
    object_io_bytes_sum: int = 0

    def add(self, event: Dict[str, Any]) -> None:
        self.count += 1
        status = str(event.get("status", "")).lower()
        if status == "fail":
            self.fail += 1
        else:
            self.success += 1

        latency_ms = as_float(event.get("latency_ms"))
        if latency_ms is not None:
            self.latency_sum_ms += latency_ms
            self.latencies_ms.append(latency_ms)

        self.event_bytes_sum += as_int(event.get("event_bytes")) or 0
        self.datanode_bytes_sum += as_int(event.get("datanode_bytes")) or 0
        self.object_io_bytes_sum += as_int(event.get("object_io_bytes")) or 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "success": self.success,
            "fail": self.fail,
            "latency_sum_ms": round(self.latency_sum_ms, 3),
            "latency_avg_ms": round(self.latency_sum_ms / len(self.latencies_ms), 3)
            if self.latencies_ms
            else None,
            "latency_p50_ms": percentile(self.latencies_ms, 50),
            "latency_p95_ms": percentile(self.latencies_ms, 95),
            "event_bytes_sum": self.event_bytes_sum,
            "datanode_bytes_sum": self.datanode_bytes_sum,
            "object_io_bytes_sum": self.object_io_bytes_sum,
        }


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil((pct / 100.0) * len(ordered)) - 1
    rank = max(0, min(rank, len(ordered) - 1))
    return round(ordered[rank], 3)


def as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().strip('"')
    try:
        return float(text)
    except ValueError:
        return None


def as_int(value: Any) -> Optional[int]:
    number = as_float(value)
    if number is None:
        return None
    return int(number)


def first_int(event: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[int]:
    for key in keys:
        value = as_int(event.get(key))
        if value is not None:
            return value
    return None


def find_json_object(line: str) -> Optional[Dict[str, Any]]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", line):
        try:
            obj, _ = decoder.raw_decode(line[match.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            msg = str(obj.get("msg", ""))
            if msg in MARKERS or any(marker in line for marker in MARKERS):
                return obj
    return None


def parse_key_values(text: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        result[key.strip()] = value.strip().strip('"')
    return result


def parse_line(line: str, source: str, line_no: int) -> Optional[Dict[str, Any]]:
    if not any(marker in line for marker in MARKERS):
        return None

    data = find_json_object(line)
    if data is None:
        marker = TRACE_MARKER if TRACE_MARKER in line else OBJECT_IO_MARKER
        _, tail = line.split(marker, 1)
        data = parse_key_values(tail)
        data["msg"] = marker

    msg = str(data.get("msg", ""))
    if msg not in MARKERS:
        msg = TRACE_MARKER if TRACE_MARKER in line else OBJECT_IO_MARKER

    event_type = "trace" if msg == TRACE_MARKER else "object_io"
    event = dict(data)
    event["source"] = source
    event["line_no"] = line_no
    event["event_type"] = event_type
    event["op"] = str(event.get("op", "unknown"))
    event["component"] = classify_component(event)
    event["path_kind"] = str(event.get("path_kind", "unknown"))
    event["status"] = str(event.get("status", "success")).lower()

    path_hit = as_bool(event.get("path_hit"))
    if path_hit is None and event_type == "object_io":
        path_hit = as_bool(event.get("object_storage_path_hit"))
    event["path_hit"] = bool(path_hit) if path_hit is not None else False
    object_storage_path_hit = as_bool(event.get("object_storage_path_hit"))
    event["object_storage_path_hit"] = (
        bool(object_storage_path_hit) if object_storage_path_hit is not None else event["path_hit"]
    )

    latency_ms = as_float(event.get("latency_ms"))
    event["latency_ms"] = latency_ms

    event["object_io_bytes"] = as_int(event.get("bytes")) or 0
    event["remote_file_count"] = first_int(event, ("remote_file_count", "datanode_remote_file_count")) or 0
    event["datanode_remote_file_count"] = event["remote_file_count"]
    event["datanode_bytes"] = get_datanode_bytes(event)
    event["datanode_remote_log_bytes"] = event["datanode_bytes"]
    event["event_bytes"] = get_event_bytes(event)

    return event


def classify_component(event: Dict[str, Any]) -> str:
    if event.get("event_type") == "object_io":
        return "object_io"
    if str(event.get("op", "")) in DATANODE_WRITE_SIDE_OPS:
        return "datanode"
    return "querynode"


def get_datanode_bytes(event: Dict[str, Any]) -> int:
    explicit = as_int(event.get("datanode_remote_log_bytes"))
    if explicit is not None:
        return explicit

    insert_bytes = as_int(event.get("insert_log_bytes")) or 0
    stats_bytes = as_int(event.get("stats_log_bytes")) or 0
    delta_bytes = as_int(event.get("delta_log_bytes")) or 0
    if insert_bytes or stats_bytes or delta_bytes:
        return insert_bytes + stats_bytes + delta_bytes

    if str(event.get("op", "")).startswith("datanode_write"):
        return as_int(event.get("remote_bytes")) or 0

    return 0


def get_event_bytes(event: Dict[str, Any]) -> int:
    for key in ("remote_bytes", "bytes"):
        value = as_int(event.get(key))
        if value is not None:
            return value
    return get_datanode_bytes(event) + (as_int(event.get("index_bytes")) or 0)


def expand_inputs(paths: List[str]) -> List[str]:
    if not paths:
        return ["-"]
    expanded: List[str] = []
    for path in paths:
        matches = glob.glob(path)
        expanded.extend(matches if matches else [path])
    return expanded


def iter_lines(paths: List[str]) -> Iterable[Tuple[str, int, str]]:
    for path in expand_inputs(paths):
        if path == "-":
            for line_no, line in enumerate(sys.stdin, 1):
                yield "-", line_no, line.rstrip("\n")
            continue
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, 1):
                yield path, line_no, line.rstrip("\n")


def model_ops(profile: str) -> Optional[set]:
    if profile == "querynode_read_side":
        return QUERYNODE_READ_SIDE_OPS
    if profile == "segment_load_topline":
        return SEGMENT_LOAD_TOPLINE_OPS
    if profile == "datanode_write_side":
        return DATANODE_WRITE_SIDE_OPS
    if profile == "all_datanode_trace":
        return None
    raise ValueError(f"unknown model profile: {profile}")


def is_datanode_path_event(event: Dict[str, Any]) -> bool:
    path_kind = str(event.get("path_kind", ""))
    return event.get("event_type") == "trace" and (
        bool(event.get("path_hit")) or path_kind.startswith("datanode")
    )


def select_model_events(events: List[Dict[str, Any]], profile: str) -> List[Dict[str, Any]]:
    selected_ops = model_ops(profile)
    selected = []
    for event in events:
        if event.get("status") != "success":
            continue
        if not is_datanode_path_event(event):
            continue
        if selected_ops is not None and event.get("op") not in selected_ops:
            continue
        selected.append(event)
    return selected


def aggregate(events: List[Dict[str, Any]]) -> Tuple[Dict[str, Agg], Dict[str, Any]]:
    groups: Dict[str, Agg] = {}
    for event in events:
        key = "|".join(
            [
                str(event.get("event_type")),
                str(event.get("op")),
                str(event.get("path_kind")),
                str(event.get("path_hit")),
                str(event.get("status")),
            ]
        )
        groups.setdefault(key, Agg()).add(event)

    trace_success = [
        e for e in events if e.get("event_type") == "trace" and e.get("status") == "success"
    ]
    trace_path_hit = [e for e in trace_success if is_datanode_path_event(e)]
    trace_no_path = [e for e in trace_success if not is_datanode_path_event(e)]
    querynode_trace_success = [
        e for e in trace_success if e.get("component") == "querynode"
    ]
    querynode_trace_path_hit = [
        e for e in querynode_trace_success if is_datanode_path_event(e)
    ]
    querynode_trace_no_path = [
        e for e in querynode_trace_success if not is_datanode_path_event(e)
    ]
    datanode_trace_success = [
        e for e in trace_success if e.get("component") == "datanode"
    ]

    totals = {
        "events": len(events),
        "trace_success_events": len(trace_success),
        "trace_success_path_hit_events": len(trace_path_hit),
        "trace_success_no_path_events": len(trace_no_path),
        "trace_path_hit_ratio": round(len(trace_path_hit) / len(trace_success), 6)
        if trace_success
        else None,
        "trace_path_hit_latency_sum_ms": round(
            sum(as_float(e.get("latency_ms")) or 0 for e in trace_path_hit), 3
        ),
        "trace_no_path_latency_sum_ms": round(
            sum(as_float(e.get("latency_ms")) or 0 for e in trace_no_path), 3
        ),
        "trace_datanode_bytes_sum": sum(as_int(e.get("datanode_bytes")) or 0 for e in trace_path_hit),
        "querynode_trace_success_events": len(querynode_trace_success),
        "querynode_trace_path_hit_events": len(querynode_trace_path_hit),
        "querynode_trace_no_path_events": len(querynode_trace_no_path),
        "querynode_path_hit_ratio": round(
            len(querynode_trace_path_hit) / len(querynode_trace_success), 6
        )
        if querynode_trace_success
        else None,
        "querynode_path_hit_latency_sum_ms": round(
            sum(as_float(e.get("latency_ms")) or 0 for e in querynode_trace_path_hit), 3
        ),
        "querynode_no_path_latency_sum_ms": round(
            sum(as_float(e.get("latency_ms")) or 0 for e in querynode_trace_no_path), 3
        ),
        "querynode_datanode_bytes_sum": sum(
            as_int(e.get("datanode_bytes")) or 0 for e in querynode_trace_path_hit
        ),
        "datanode_write_success_events": len(datanode_trace_success),
        "datanode_write_latency_sum_ms": round(
            sum(as_float(e.get("latency_ms")) or 0 for e in datanode_trace_success), 3
        ),
        "datanode_write_bytes_sum": sum(
            as_int(e.get("datanode_bytes")) or 0 for e in datanode_trace_success
        ),
        "object_io_events": len([e for e in events if e.get("event_type") == "object_io"]),
        "object_io_bytes_sum": sum(
            as_int(e.get("object_io_bytes")) or 0
            for e in events
            if e.get("event_type") == "object_io"
        ),
    }
    return groups, totals


def build_model(
    events: List[Dict[str, Any]],
    profile: str,
    base_ms: Optional[float],
    optimized_path_ms: Optional[float],
    path_speedup: Optional[float],
    qps_base: Optional[float],
) -> Dict[str, Any]:
    selected = select_model_events(events, profile)
    old_path_ms = sum(as_float(e.get("latency_ms")) or 0 for e in selected)

    new_path_ms = None
    if optimized_path_ms is not None:
        new_path_ms = optimized_path_ms
    elif path_speedup is not None and path_speedup > 0:
        new_path_ms = old_path_ms / path_speedup

    model: Dict[str, Any] = {
        "profile": profile,
        "selected_event_count": len(selected),
        "old_path_latency_sum_ms": round(old_path_ms, 3),
        "old_path_datanode_bytes_sum": sum(as_int(e.get("datanode_bytes")) or 0 for e in selected),
        "new_path_latency_sum_ms": round(new_path_ms, 3) if new_path_ms is not None else None,
        "warnings": [],
    }

    if base_ms is not None and base_ms > 0:
        model["old_path_fraction_of_base"] = round(old_path_ms / base_ms, 6)
        if old_path_ms > base_ms:
            model["warnings"].append(
                "selected path latency is larger than base total time; check whether overlapping events were double-counted"
            )

    if base_ms is not None and new_path_ms is not None:
        new_total = base_ms - old_path_ms + new_path_ms
        model["base_total_ms"] = round(base_ms, 3)
        model["new_total_ms"] = round(new_total, 3)
        model["saved_ms"] = round(old_path_ms - new_path_ms, 3)
        if base_ms > 0 and new_total > 0:
            model["estimated_speedup"] = round(base_ms / new_total, 6)
            model["estimated_improvement_percent"] = round((base_ms / new_total - 1) * 100, 3)
        if qps_base is not None and new_total > 0:
            model["qps_base"] = qps_base
            model["qps_estimated"] = round(qps_base * base_ms / new_total, 6)

    return model


def write_events_csv(path: str, events: List[Dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_CSV_FIELDS)
        writer.writeheader()
        for event in events:
            writer.writerow({field: event.get(field, "") for field in EVENT_CSV_FIELDS})


def write_ops_csv(path: str, groups: Dict[str, Agg]) -> None:
    fields = [
        "event_type",
        "op",
        "path_kind",
        "path_hit",
        "status",
        "count",
        "success",
        "fail",
        "latency_sum_ms",
        "latency_avg_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "event_bytes_sum",
        "datanode_bytes_sum",
        "object_io_bytes_sum",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for key in sorted(groups):
            event_type, op, path_kind, path_hit, status = key.split("|", 4)
            row = {
                "event_type": event_type,
                "op": op,
                "path_kind": path_kind,
                "path_hit": path_hit,
                "status": status,
            }
            row.update(groups[key].to_dict())
            writer.writerow(row)


def print_human_report(totals: Dict[str, Any], groups: Dict[str, Agg], model: Dict[str, Any]) -> None:
    print("Milvus QPS path trace summary")
    print(f"  events: {totals['events']}")
    print(f"  trace success events: {totals['trace_success_events']}")
    print(f"  datanode path hit events: {totals['trace_success_path_hit_events']}")
    print(f"  no-path events: {totals['trace_success_no_path_events']}")
    print(f"  path hit ratio: {totals['trace_path_hit_ratio']}")
    print(f"  datanode path latency sum ms: {totals['trace_path_hit_latency_sum_ms']}")
    print(f"  no-path latency sum ms: {totals['trace_no_path_latency_sum_ms']}")
    print(f"  datanode bytes sum: {totals['trace_datanode_bytes_sum']}")
    print(f"  object I/O events: {totals['object_io_events']}")
    print(f"  object I/O bytes sum: {totals['object_io_bytes_sum']}")
    print("")
    print("QueryNode trace only")
    print(f"  trace success events: {totals['querynode_trace_success_events']}")
    print(f"  datanode path hit events: {totals['querynode_trace_path_hit_events']}")
    print(f"  no-path events: {totals['querynode_trace_no_path_events']}")
    print(f"  path hit ratio: {totals['querynode_path_hit_ratio']}")
    print(f"  datanode path latency sum ms: {totals['querynode_path_hit_latency_sum_ms']}")
    print(f"  no-path latency sum ms: {totals['querynode_no_path_latency_sum_ms']}")
    print(f"  datanode bytes sum: {totals['querynode_datanode_bytes_sum']}")
    print("")
    print("DataNode write trace only")
    print(f"  write success events: {totals['datanode_write_success_events']}")
    print(f"  write latency sum ms: {totals['datanode_write_latency_sum_ms']}")
    print(f"  write bytes sum: {totals['datanode_write_bytes_sum']}")
    print("")
    print(f"Model profile: {model['profile']}")
    print(f"  selected events: {model['selected_event_count']}")
    print(f"  old path latency sum ms: {model['old_path_latency_sum_ms']}")
    print(f"  old path datanode bytes sum: {model['old_path_datanode_bytes_sum']}")
    if model.get("old_path_fraction_of_base") is not None:
        print(f"  old path fraction of base: {model['old_path_fraction_of_base']}")
    if model.get("new_path_latency_sum_ms") is not None:
        print(f"  new path latency sum ms: {model['new_path_latency_sum_ms']}")
    if model.get("estimated_improvement_percent") is not None:
        print(f"  base total ms: {model['base_total_ms']}")
        print(f"  new total ms: {model['new_total_ms']}")
        print(f"  estimated speedup: {model['estimated_speedup']}")
        print(f"  estimated improvement percent: {model['estimated_improvement_percent']}")
    if model.get("qps_estimated") is not None:
        print(f"  qps base: {model['qps_base']}")
        print(f"  qps estimated: {model['qps_estimated']}")
    for warning in model.get("warnings", []):
        print(f"  warning: {warning}")
    print("")
    print("Top groups by latency_sum_ms:")
    top = sorted(groups.items(), key=lambda item: item[1].latency_sum_ms, reverse=True)[:12]
    for key, agg in top:
        print(f"  {key}: {agg.to_dict()}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse Milvus qps path profiling logs.")
    parser.add_argument("logs", nargs="*", help="Log files or glob patterns. Use '-' or omit for stdin.")
    parser.add_argument("--summary-json", help="Write full summary JSON to this path.")
    parser.add_argument("--events-csv", help="Write normalized events CSV to this path.")
    parser.add_argument("--ops-csv", help="Write grouped operation summary CSV to this path.")
    parser.add_argument(
        "--model-profile",
        choices=[
            "querynode_read_side",
            "segment_load_topline",
            "datanode_write_side",
            "all_datanode_trace",
        ],
        default="querynode_read_side",
        help="Latency set used for what-if modeling. Default avoids double-counting load_segment and child events.",
    )
    parser.add_argument("--base-total-ms", type=float, help="Baseline end-to-end total time in milliseconds.")
    parser.add_argument("--optimized-path-ms", type=float, help="Measured optimized path time in milliseconds.")
    parser.add_argument("--path-speedup", type=float, help="Path speedup ratio when optimized-path-ms is unavailable.")
    parser.add_argument("--qps-base", type=float, help="Baseline QPS for estimated QPS output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    events = []
    for source, line_no, line in iter_lines(args.logs):
        event = parse_line(line, source, line_no)
        if event is not None:
            events.append(event)

    groups, totals = aggregate(events)
    model = build_model(
        events,
        args.model_profile,
        args.base_total_ms,
        args.optimized_path_ms,
        args.path_speedup,
        args.qps_base,
    )
    result = {
        "totals": totals,
        "model": model,
        "groups": {key: value.to_dict() for key, value in sorted(groups.items())},
    }

    if args.events_csv:
        write_events_csv(args.events_csv, events)
    if args.ops_csv:
        write_ops_csv(args.ops_csv, groups)
    if args.summary_json:
        Path(args.summary_json).write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    print_human_report(totals, groups, model)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
