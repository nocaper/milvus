#!/usr/bin/env python3
"""Analyze QueryNode remote object load traces.

The script accepts Milvus JSON logs and the C++ text logs produced by the
QueryNode/segcore trace points added for v2.4.5. It focuses on object-storage
reads for segment binlogs, index files, stats logs, delta logs, and raw data.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


REMOTE_TRACE_MSG = "querynode remote object fetch trace"
LEGACY_V1_MSG = "segcore download object from remote storage"
LEGACY_V2_MSG = "segcore download storage v2 blob from remote storage"
SEGMENT_VA_MSG = "querynode segment field data va trace"
CHUNK_CACHE_VA_MSG = "querynode chunk cache mmap va trace"

PAIR_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^,]*)(?:,|$)")


def to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def mib(value: float) -> float:
    return value / 1024 / 1024


def parse_pairs(text: str) -> Dict[str, str]:
    return {match.group(1): match.group(2).strip() for match in PAIR_RE.finditer(text)}


def normalize_path(remote_path: str) -> List[str]:
    return [part for part in remote_path.replace("\\", "/").split("/") if part]


def parse_remote_path(remote_path: str) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "remote_kind": "unknown",
        "collection_id": -1,
        "partition_id": -1,
        "segment_id": -1,
        "field_id": -1,
        "log_id": -1,
        "build_id": -1,
        "index_version": -1,
        "path_base": Path(remote_path.replace("\\", "/")).name,
    }
    parts = normalize_path(remote_path)
    for idx, part in enumerate(parts):
        if part in {"insert_log", "stats_log"} and idx + 5 < len(parts):
            info["remote_kind"] = "segment" if part == "insert_log" else "stats"
            info["collection_id"] = to_int(parts[idx + 1], -1)
            info["partition_id"] = to_int(parts[idx + 2], -1)
            info["segment_id"] = to_int(parts[idx + 3], -1)
            info["field_id"] = to_int(parts[idx + 4], -1)
            info["log_id"] = to_int(parts[idx + 5], -1)
            return info
        if part == "delta_log" and idx + 4 < len(parts):
            info["remote_kind"] = "delta"
            info["collection_id"] = to_int(parts[idx + 1], -1)
            info["partition_id"] = to_int(parts[idx + 2], -1)
            info["segment_id"] = to_int(parts[idx + 3], -1)
            info["log_id"] = to_int(parts[idx + 4], -1)
            return info
        if part == "index_files" and idx + 4 < len(parts):
            info["remote_kind"] = "index"
            info["build_id"] = to_int(parts[idx + 1], -1)
            info["index_version"] = to_int(parts[idx + 2], -1)
            info["partition_id"] = to_int(parts[idx + 3], -1)
            info["segment_id"] = to_int(parts[idx + 4], -1)
            return info
        if part == "raw_datas" and idx + 2 < len(parts):
            info["remote_kind"] = "raw_data"
            info["segment_id"] = to_int(parts[idx + 1], -1)
            info["field_id"] = to_int(parts[idx + 2], -1)
            if idx + 3 < len(parts):
                info["log_id"] = to_int(parts[idx + 3], -1)
            return info
    return info


def log_message(record: Dict[str, Any], raw_line: str) -> str:
    return str(record.get("msg") or record.get("message") or record.get("M") or raw_line)


def parse_json_line(line: str) -> Optional[Dict[str, Any]]:
    stripped = line.strip()
    if not stripped.startswith("{"):
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def merge_path_info(record: Dict[str, Any]) -> Dict[str, Any]:
    remote_path = str(record.get("remote_path") or record.get("file") or record.get("blob") or "")
    info = parse_remote_path(remote_path) if remote_path else {}
    merged = dict(info)
    merged.update(record)
    if remote_path and not merged.get("remote_path"):
        merged["remote_path"] = remote_path
    if "remote_kind" not in merged or merged.get("remote_kind") in {"", None}:
        merged["remote_kind"] = info.get("remote_kind", "unknown")
    return merged


def make_remote_record(data: Dict[str, Any], source: str, line_no: int, legacy: bool = False) -> Dict[str, Any]:
    merged = merge_path_info(data)
    encoded = merged.get("encoded_bytes")
    if encoded is None:
        encoded = merged.get("size")
    record = {
        "source": source,
        "line": line_no,
        "legacy": legacy,
        "event": str(merged.get("event") or ("legacy_size_done" if legacy else "")),
        "storage_version": str(merged.get("storage_version") or merged.get("storageVersion") or ""),
        "remote_kind": str(merged.get("remote_kind") or "unknown"),
        "remote_path": str(merged.get("remote_path") or merged.get("file") or merged.get("blob") or ""),
        "path_base": str(merged.get("path_base") or ""),
        "collection_id": to_int(merged.get("collection_id", merged.get("collectionID", -1)), -1),
        "partition_id": to_int(merged.get("partition_id", merged.get("partitionID", -1)), -1),
        "segment_id": to_int(merged.get("segment_id", merged.get("segmentID", -1)), -1),
        "field_id": to_int(merged.get("field_id", merged.get("fieldID", -1)), -1),
        "log_id": to_int(merged.get("log_id", -1), -1),
        "build_id": to_int(merged.get("build_id", merged.get("buildID", -1)), -1),
        "index_version": to_int(merged.get("index_version", merged.get("indexVersion", -1)), -1),
        "encoded_bytes": to_int(encoded, 0),
        "decoded_bytes": to_int(merged.get("decoded_bytes"), 0),
        "row_count": to_int(merged.get("row_count", merged.get("rowCount", 0)), 0),
        "dim": to_int(merged.get("dim"), 0),
        "data_type": str(merged.get("data_type") or ""),
        "duration_ms": to_float(merged.get("duration_ms"), 0.0),
        "batch_duration_ms": to_float(merged.get("batch_duration_ms"), 0.0),
        "caller": str(merged.get("caller") or ""),
    }
    if record["remote_kind"] == "unknown" and record["remote_path"]:
        record.update(parse_remote_path(record["remote_path"]))
    return record


def extract_records_from_json(data: Dict[str, Any], raw_line: str, source: str, line_no: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    msg = log_message(data, raw_line)
    remote_records: List[Dict[str, Any]] = []
    planned_records: List[Dict[str, Any]] = []
    va_records: List[Dict[str, Any]] = []

    if REMOTE_TRACE_MSG in msg:
        remote_records.append(make_remote_record(data, source, line_no))
        return remote_records, planned_records, va_records

    if LEGACY_V1_MSG in msg:
        remote_path = data.get("file") or data.get("remote_path") or ""
        size = data.get("size") or data.get("encoded_bytes") or 0
        remote_records.append(make_remote_record({
            "event": "legacy_size_done",
            "storage_version": "v1",
            "remote_path": remote_path,
            "encoded_bytes": size,
        }, source, line_no, legacy=True))

    if LEGACY_V2_MSG in msg:
        remote_path = data.get("blob") or data.get("remote_path") or ""
        size = data.get("size") or data.get("encoded_bytes") or 0
        remote_records.append(make_remote_record({
            "event": "legacy_size_done",
            "storage_version": "v2",
            "remote_path": remote_path,
            "encoded_bytes": size,
        }, source, line_no, legacy=True))

    if "loadObjectSummary" in data and isinstance(data["loadObjectSummary"], dict):
        planned_records.extend(extract_planned_summary(data, source, line_no, msg))

    if SEGMENT_VA_MSG in msg or CHUNK_CACHE_VA_MSG in msg:
        va_records.append(make_va_record(data, source, line_no, msg))

    return remote_records, planned_records, va_records


def extract_records_from_text(line: str, source: str, line_no: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    remote_records: List[Dict[str, Any]] = []
    planned_records: List[Dict[str, Any]] = []
    va_records: List[Dict[str, Any]] = []

    if REMOTE_TRACE_MSG in line:
        payload = line.split(REMOTE_TRACE_MSG, 1)[1]
        if payload.startswith(","):
            payload = payload[1:]
        remote_records.append(make_remote_record(parse_pairs(payload), source, line_no))

    if LEGACY_V1_MSG in line:
        match = re.search(r"file=([^,]+),\s*size=(\d+)", line)
        if match:
            remote_records.append(make_remote_record({
                "event": "legacy_size_done",
                "storage_version": "v1",
                "remote_path": match.group(1),
                "encoded_bytes": match.group(2),
            }, source, line_no, legacy=True))

    if LEGACY_V2_MSG in line:
        match = re.search(r"blob=([^,]+),\s*size=(\d+)", line)
        if match:
            remote_records.append(make_remote_record({
                "event": "legacy_size_done",
                "storage_version": "v2",
                "remote_path": match.group(1),
                "encoded_bytes": match.group(2),
            }, source, line_no, legacy=True))

    if SEGMENT_VA_MSG in line:
        payload = line.split(SEGMENT_VA_MSG, 1)[1]
        if payload.startswith(","):
            payload = payload[1:]
        data = parse_pairs(payload)
        data["msg"] = SEGMENT_VA_MSG
        va_records.append(make_va_record(data, source, line_no, SEGMENT_VA_MSG))

    if CHUNK_CACHE_VA_MSG in line:
        payload = line.split(CHUNK_CACHE_VA_MSG, 1)[1]
        if payload.startswith(","):
            payload = payload[1:]
        data = parse_pairs(payload)
        data["msg"] = CHUNK_CACHE_VA_MSG
        va_records.append(make_va_record(data, source, line_no, CHUNK_CACHE_VA_MSG))

    return remote_records, planned_records, va_records


def get_nested(summary: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = summary.get(key) or {}
    return value if isinstance(value, dict) else {}


def extract_planned_summary(data: Dict[str, Any], source: str, line_no: int, msg: str) -> List[Dict[str, Any]]:
    summary = data["loadObjectSummary"]
    base = {
        "source": source,
        "line": line_no,
        "msg": msg,
        "collection_id": to_int(data.get("collectionID", summary.get("collection_id", -1)), -1),
        "partition_id": to_int(data.get("partitionID", summary.get("partition_id", -1)), -1),
        "segment_id": to_int(data.get("segmentID", summary.get("segment_id", -1)), -1),
        "row_count": to_int(data.get("rowCount", summary.get("row_count", 0)), 0),
        "storage_version": str(data.get("storageVersion", summary.get("storage_version", ""))),
    }
    result: List[Dict[str, Any]] = []
    for remote_kind, key, size_key in [
        ("segment", "insert", "log_size"),
        ("stats", "stats", "log_size"),
        ("delta", "delta", "log_size"),
        ("index", "index", "index_size"),
    ]:
        item = get_nested(summary, key)
        file_count = to_int(item.get("file_count"), 0)
        total_bytes = to_int(item.get(size_key), 0)
        if file_count == 0 and total_bytes == 0:
            continue
        row = dict(base)
        row.update({
            "remote_kind": remote_kind,
            "file_count": file_count,
            "planned_bytes": total_bytes,
            "memory_size": to_int(item.get("memory_size"), 0),
            "entry_count": to_int(item.get("entry_count"), 0),
            "path_sample": str(item.get("path_sample") or ""),
        })
        result.append(row)
    return result


def make_va_record(data: Dict[str, Any], source: str, line_no: int, msg: str) -> Dict[str, Any]:
    return {
        "source": source,
        "line": line_no,
        "msg": msg,
        "segment_id": to_int(data.get("segment_id", data.get("segmentID", -1)), -1),
        "field_id": to_int(data.get("field_id", data.get("fieldID", -1)), -1),
        "load_mode": str(data.get("load_mode") or data.get("loadMode") or ""),
        "remote_kind": "chunk_cache" if CHUNK_CACHE_VA_MSG in msg else "segment",
        "segment_length_bytes": to_int(data.get("segment_length_bytes"), 0),
        "column_byte_size": to_int(data.get("column_byte_size"), 0),
        "source_bytes": to_int(data.get("source_bytes"), 0),
        "row_count": to_int(data.get("row_count", data.get("rows", data.get("column_rows", 0))), 0),
        "data_va": str(data.get("data_va") or ""),
        "mmap_va": str(data.get("mmap_va") or ""),
        "mmap_file": str(data.get("mmap_file") or data.get("cache_file") or ""),
    }


def iter_input_files(patterns: Sequence[str]) -> Iterable[Path]:
    seen = set()
    for pattern in patterns:
        matches = glob.glob(pattern, recursive=True)
        if not matches:
            matches = [pattern]
        for match in matches:
            path = Path(match)
            if path in seen:
                continue
            seen.add(path)
            if path.is_file():
                yield path


def parse_logs(files: Sequence[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int]:
    remote_records: List[Dict[str, Any]] = []
    planned_records: List[Dict[str, Any]] = []
    va_records: List[Dict[str, Any]] = []
    line_count = 0

    for file_path in iter_input_files(files):
        with file_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, 1):
                line_count += 1
                data = parse_json_line(line)
                if data is not None:
                    remote, planned, va = extract_records_from_json(data, line, str(file_path), line_no)
                else:
                    remote, planned, va = extract_records_from_text(line, str(file_path), line_no)
                remote_records.extend(remote)
                planned_records.extend(planned)
                va_records.extend(va)

    return remote_records, planned_records, va_records, line_count


def percentile(sorted_values: Sequence[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[int(rank)]
    weight = rank - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def stats(values: Sequence[float]) -> Dict[str, float]:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return {key: 0.0 for key in ["count", "sum", "min", "max", "mean", "stddev", "p50", "p90", "p95", "p99"]}
    ordered = sorted(cleaned)
    return {
        "count": float(len(cleaned)),
        "sum": sum(cleaned),
        "min": ordered[0],
        "max": ordered[-1],
        "mean": statistics.fmean(cleaned),
        "stddev": statistics.pstdev(cleaned) if len(cleaned) > 1 else 0.0,
        "p50": percentile(ordered, 50),
        "p90": percentile(ordered, 90),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
    }


def group_by(records: Sequence[Dict[str, Any]], keys: Sequence[str]) -> Dict[Tuple[Any, ...], List[Dict[str, Any]]]:
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[tuple(record.get(key, "") for key in keys)].append(record)
    return grouped


def summarize_group(records: Sequence[Dict[str, Any]], byte_key: str = "encoded_bytes") -> Dict[str, Any]:
    byte_values = [to_float(record.get(byte_key), 0.0) for record in records]
    duration_values = []
    for record in records:
        duration = to_float(record.get("duration_ms"), 0.0)
        if duration <= 0:
            batch_duration = to_float(record.get("batch_duration_ms"), 0.0)
            batch_count = max(to_int(record.get("batch_object_count"), 1), 1)
            duration = batch_duration / batch_count if batch_duration > 0 else 0.0
        if duration > 0:
            duration_values.append(duration)
    byte_stats = stats(byte_values)
    duration_stats = stats(duration_values)
    total_duration_ms = sum(duration_values)
    throughput = mib(byte_stats["sum"]) / (total_duration_ms / 1000.0) if total_duration_ms > 0 else 0.0
    return {
        "count": int(byte_stats["count"]),
        "total_mib": mib(byte_stats["sum"]),
        "mean_kib": byte_stats["mean"] / 1024,
        "p50_kib": byte_stats["p50"] / 1024,
        "p95_kib": byte_stats["p95"] / 1024,
        "max_kib": byte_stats["max"] / 1024,
        "duration_mean_ms": duration_stats["mean"],
        "duration_p95_ms": duration_stats["p95"],
        "throughput_mib_s": throughput,
    }


def print_group_table(title: str, grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]], keys: Sequence[str], top: int, byte_key: str = "encoded_bytes") -> None:
    print(f"\n{title}")
    print("-" * len(title))
    rows = []
    for group_key, records in grouped.items():
        summary = summarize_group(records, byte_key)
        rows.append((summary["total_mib"], group_key, summary))
    rows.sort(reverse=True, key=lambda item: item[0])
    if not rows:
        print("(no data)")
        return
    header = " | ".join([*keys, "count", "total_mib", "mean_kib", "p95_kib", "dur_p95_ms", "mib_s"])
    print(header)
    print("-" * len(header))
    for _, group_key, summary in rows[:top]:
        values = [str(value) for value in group_key]
        values.extend([
            str(summary["count"]),
            f"{summary['total_mib']:.2f}",
            f"{summary['mean_kib']:.1f}",
            f"{summary['p95_kib']:.1f}",
            f"{summary['duration_p95_ms']:.2f}",
            f"{summary['throughput_mib_s']:.2f}",
        ])
        print(" | ".join(values))


def print_report(remote_records: List[Dict[str, Any]], planned_records: List[Dict[str, Any]], va_records: List[Dict[str, Any]], line_count: int, top: int) -> None:
    read_records = [record for record in remote_records if record["event"] in {"read_done", "legacy_size_done"}]
    decoded_records = [record for record in remote_records if record["event"] == "decode_done"]
    new_read_records = [record for record in read_records if not record.get("legacy")]

    print("QueryNode MinIO/Object Storage Trace Summary")
    print("===========================================")
    print(f"scanned_lines: {line_count}")
    print(f"remote_trace_records: {len(remote_records)}")
    print(f"actual_or_legacy_read_records: {len(read_records)}")
    print(f"new_read_done_records: {len(new_read_records)}")
    print(f"decode_done_records: {len(decoded_records)}")
    print(f"planned_summary_records: {len(planned_records)}")
    print(f"va_records: {len(va_records)}")

    if read_records:
        summary = summarize_group(read_records)
        print(f"\nread_total_mib: {summary['total_mib']:.2f}")
        print(f"read_mean_kib: {summary['mean_kib']:.1f}")
        print(f"read_p95_kib: {summary['p95_kib']:.1f}")
        print(f"read_duration_p95_ms: {summary['duration_p95_ms']:.2f}")
        print(f"read_throughput_mib_s: {summary['throughput_mib_s']:.2f}")

    print_group_table("Actual Reads By Object Kind", group_by(read_records, ["remote_kind"]), ["remote_kind"], top)
    print_group_table("Actual Reads By Segment", group_by([r for r in read_records if r.get("segment_id", -1) >= 0], ["segment_id"]), ["segment_id"], top)
    print_group_table("Actual Index Reads By Build", group_by([r for r in read_records if r.get("remote_kind") == "index"], ["build_id", "index_version", "segment_id"]), ["build_id", "index_version", "segment_id"], top)

    if decoded_records:
        print_group_table("Decoded Payload By Object Kind", group_by(decoded_records, ["remote_kind"]), ["remote_kind"], top, byte_key="decoded_bytes")

    if planned_records:
        planned_as_records = [
            {
                "remote_kind": row["remote_kind"],
                "segment_id": row["segment_id"],
                "encoded_bytes": row["planned_bytes"],
                "duration_ms": 0,
            }
            for row in planned_records
        ]
        print_group_table("Planned Metadata Bytes By Object Kind", group_by(planned_as_records, ["remote_kind"]), ["remote_kind"], top)

    if va_records:
        va_as_records = [
            {
                "remote_kind": row["remote_kind"],
                "segment_id": row["segment_id"],
                "load_mode": row["load_mode"],
                "encoded_bytes": row["segment_length_bytes"],
                "duration_ms": 0,
            }
            for row in va_records
        ]
        print_group_table("Loaded VA Length By Mode", group_by(va_as_records, ["remote_kind", "load_mode"]), ["remote_kind", "load_mode"], top)


def write_csv(path: str, records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return
    fields = sorted({key for record in records for key in record.keys()})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Milvus QueryNode MinIO/object-storage trace logs.")
    parser.add_argument("logs", nargs="+", help="Log file paths or glob patterns.")
    parser.add_argument("--top", type=int, default=20, help="Rows to show for grouped tables.")
    parser.add_argument("--csv-prefix", help="Write remote/planned/va CSV files using this prefix.")
    parser.add_argument("--json-out", help="Write parsed records as JSON.")
    args = parser.parse_args()

    remote_records, planned_records, va_records, line_count = parse_logs(args.logs)
    print_report(remote_records, planned_records, va_records, line_count, args.top)

    if args.csv_prefix:
        write_csv(f"{args.csv_prefix}.remote.csv", remote_records)
        write_csv(f"{args.csv_prefix}.planned.csv", planned_records)
        write_csv(f"{args.csv_prefix}.va.csv", va_records)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump({
                "remote": remote_records,
                "planned": planned_records,
                "va": va_records,
            }, handle, ensure_ascii=False, indent=2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
