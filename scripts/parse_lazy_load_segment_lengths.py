#!/usr/bin/env python3

import argparse
import csv
import json
import sys
from collections import defaultdict


SEGMENT_EVENT = "lazy_load_segment_length"
INDEX_EVENT = "lazy_load_index_length"
OBJECT_EVENT = "lazy_load_storage_object_deserialized"


def iter_json_objects(text):
    decoder = json.JSONDecoder()
    idx = 0
    while True:
        start = text.find("{", idx)
        if start < 0:
            return
        try:
            obj, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            idx = start + 1
            continue
        yield obj
        idx = start + end


def walk_value(value, seen):
    if isinstance(value, dict):
        event = value.get("event")
        if event in {SEGMENT_EVENT, INDEX_EVENT, OBJECT_EVENT}:
            key = json.dumps(value, sort_keys=True, ensure_ascii=False)
            if key not in seen:
                seen.add(key)
                yield value
        for child in value.values():
            yield from walk_value(child, seen)
    elif isinstance(value, list):
        for item in value:
            yield from walk_value(item, seen)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return
            yield from walk_value(parsed, seen)


def extract_events_from_line(line):
    seen = set()
    for obj in iter_json_objects(line):
        yield from walk_value(obj, seen)


def to_int(value, default=0):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    return int(value)


def to_bool(value, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def bool_text(value, default=None):
    if value is None or value == "":
        return "" if default is None else str(bool(default)).lower()
    return str(to_bool(value)).lower()


def to_str(value):
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ";".join(str(item) for item in value)
    return str(value)


def normalize_segment_event(event):
    segment_length_bytes = event.get("segment_length_bytes")
    if segment_length_bytes is None:
        segment_length_bytes = event.get("column_bytes")
    if segment_length_bytes is None:
        segment_length_bytes = event.get("deserialized_bytes", 0)

    return {
        "data_source": "raw",
        "segment_id": to_int(event.get("segment_id")),
        "field_id": to_int(event.get("field_id")),
        "field_name": to_str(event.get("field_name")),
        "data_type": to_str(event.get("data_type")),
        "dim": to_int(event.get("dim")),
        "expected_rows": to_int(event.get("expected_rows")),
        "loaded_rows": to_int(event.get("loaded_rows")),
        "chunk_count": to_int(event.get("chunk_count")),
        "deserialized_rows": to_int(event.get("deserialized_rows")),
        "deserialized_bytes": to_int(event.get("deserialized_bytes")),
        "column_bytes": to_int(event.get("column_bytes")),
        "segment_length_bytes": to_int(segment_length_bytes),
        "load_mode": to_str(event.get("load_mode")),
        "mmap": bool_text(event.get("mmap"), default=False),
        "mmap_requested": bool_text(event.get("mmap_requested")),
        "storage_object_count": to_int(event.get("storage_object_count")),
        "storage_entries_total": to_int(event.get("storage_entries_total")),
        "storage_objects": to_str(event.get("storage_objects")),
        "storage_uri": to_str(event.get("storage_uri")),
        "storage_version": to_int(event.get("storage_version")),
        "index_id": 0,
        "index_build_id": 0,
        "index_version": 0,
        "index_type": "",
        "index_length_bytes": 0,
    }


def normalize_object_event(event):
    data_source = to_str(event.get("data_source", "raw"))
    included_in_index_length = event.get("included_in_index_length")
    if included_in_index_length is None or included_in_index_length == "":
        included_in_index_length = data_source == "index"
    return {
        "data_source": data_source,
        "segment_id": to_int(event.get("segment_id")),
        "field_id": to_int(event.get("field_id")),
        "index_id": to_int(event.get("index_id")),
        "index_build_id": to_int(event.get("index_build_id")),
        "index_version": to_int(event.get("index_version")),
        "index_type": to_str(event.get("index_type")),
        "object_kind": to_str(event.get("object_kind", "field_data")),
        "included_in_index_length": bool_text(
            included_in_index_length, default=False
        ),
        "object_path": to_str(event.get("object_path")),
        "storage_uri": to_str(event.get("storage_uri")),
        "storage_version": to_int(event.get("storage_version")),
        "serialized_bytes": to_int(event.get("serialized_bytes"), -1),
        "deserialized_bytes": to_int(event.get("deserialized_bytes")),
        "deserialized_rows": to_int(event.get("deserialized_rows")),
        "deserialized_length": to_int(event.get("deserialized_length")),
        "deserialized_dim": to_int(event.get("deserialized_dim")),
        "data_type": to_str(event.get("data_type")),
    }


def normalize_index_event(event):
    return {
        "data_source": "index",
        "segment_id": to_int(event.get("segment_id")),
        "field_id": to_int(event.get("field_id")),
        "field_name": to_str(event.get("field_name")),
        "data_type": to_str(event.get("data_type")),
        "dim": to_int(event.get("dim")),
        "expected_rows": to_int(event.get("expected_rows")),
        "loaded_rows": to_int(event.get("loaded_rows")),
        "index_id": to_int(event.get("index_id")),
        "index_build_id": to_int(event.get("index_build_id")),
        "index_version": to_int(event.get("index_version")),
        "index_type": to_str(event.get("index_type")),
        "serialized_bytes": to_int(event.get("serialized_bytes")),
        "deserialized_bytes": to_int(event.get("deserialized_bytes")),
        "index_length_bytes": to_int(
            event.get("index_length_bytes", event.get("deserialized_bytes", 0))
        ),
        "storage_object_count": to_int(event.get("storage_object_count")),
        "storage_objects": to_str(event.get("storage_objects")),
        "storage_uri": to_str(event.get("storage_uri")),
        "storage_version": to_int(event.get("storage_version")),
        "load_mode": to_str(event.get("load_mode")),
        "mmap": bool_text(event.get("mmap"), default=False),
        "mmap_requested": bool_text(event.get("mmap_requested")),
        "chunk_count": 0,
        "deserialized_rows": 0,
        "column_bytes": 0,
        "segment_length_bytes": 0,
        "storage_entries_total": 0,
    }


def read_lines(paths):
    if not paths:
        for line in sys.stdin:
            yield line
        return
    for path in paths:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            yield from fh


def write_csv(path, rows, fieldnames):
    if path:
        fh = open(path, "w", newline="", encoding="utf-8")
        close_fh = True
    else:
        fh = sys.stdout
        close_fh = False
    try:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    finally:
        if close_fh:
            fh.close()


def write_text(path, text):
    if path:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
    else:
        sys.stdout.write(text)


def format_count(value):
    return f"{value:,}"


def format_bytes(value):
    if value is None or value < 0:
        return "unknown"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(amount):,} B"
            return f"{amount:,.2f} {unit}"
        amount /= 1024


def format_ratio(numerator, denominator):
    if denominator <= 0:
        return "unknown"
    return f"{numerator / denominator:,.1f}x"


def format_field_title(row):
    parts = [f"field {row['field_id']}"]
    field_name = row.get("field_name")
    if field_name:
        parts.append(f"({field_name})")
    meta = []
    data_type = row.get("data_type")
    if data_type:
        meta.append(data_type)
    index_type = row.get("index_type")
    if index_type:
        meta.append(index_type)
    if meta:
        parts.append(f"[{'/'.join(meta)}]")
    return " ".join(parts)


def build_text_report(summary_rows, segment_rows, object_rows):
    summary_by_segment = {row["segment_id"]: row for row in summary_rows}
    fields_by_segment = defaultdict(list)
    for row in segment_rows:
        fields_by_segment[row["segment_id"]].append(row)

    object_stats = defaultdict(
        lambda: {
            "count": 0,
            "serialized_bytes": 0,
            "deserialized_bytes": 0,
            "unknown_serialized_count": 0,
        }
    )
    metadata_stats = defaultdict(
        lambda: {
            "count": 0,
            "serialized_bytes": 0,
            "deserialized_bytes": 0,
        }
    )
    for row in object_rows:
        segment_id = row["segment_id"]
        if row["data_source"] == "index_metadata":
            bucket = metadata_stats[segment_id]
            bucket["count"] += 1
            bucket["serialized_bytes"] += max(row["serialized_bytes"], 0)
            bucket["deserialized_bytes"] += max(row["deserialized_bytes"], 0)
            continue

        key = (segment_id, row["data_source"], row["field_id"])
        bucket = object_stats[key]
        bucket["count"] += 1
        if row["serialized_bytes"] < 0:
            bucket["unknown_serialized_count"] += 1
        else:
            bucket["serialized_bytes"] += row["serialized_bytes"]
        bucket["deserialized_bytes"] += max(row["deserialized_bytes"], 0)

    total_raw = sum(row["raw_segment_length_bytes_total"] for row in summary_rows)
    total_index = sum(row["index_length_bytes_total"] for row in summary_rows)
    total_decoded = total_raw + total_index
    total_raw_objects = sum(row["raw_storage_object_count_total"] for row in summary_rows)
    total_index_objects = sum(
        row["index_storage_object_count_total"] for row in summary_rows
    )

    lines = []
    lines.append("Lazy-load segment length report")
    lines.append(
        "Meaning: raw decoded is the loaded field column size after "
        "deserialization; index decoded is the index payload size after "
        "DeserializeFileData; index metadata is reported separately."
    )
    lines.append(
        f"Segments: {len(summary_rows)}, raw decoded total: {format_bytes(total_raw)}, "
        f"index decoded total: {format_bytes(total_index)}, total decoded: {format_bytes(total_decoded)}"
    )
    lines.append(
        f"Storage objects: raw {format_count(total_raw_objects)}, "
        f"index {format_count(total_index_objects)}"
    )
    lines.append("")

    for summary in summary_rows:
        segment_id = summary["segment_id"]
        segment_fields = sorted(
            fields_by_segment.get(segment_id, []),
            key=lambda row: (row["data_source"], row["field_id"]),
        )
        raw_rows = [row for row in segment_fields if row["data_source"] == "raw"]
        index_rows = [row for row in segment_fields if row["data_source"] == "index"]
        raw_total = summary["raw_segment_length_bytes_total"]
        index_total = summary["index_length_bytes_total"]
        decoded_total = raw_total + index_total

        lines.append(f"Segment {segment_id}")
        lines.append(f"  rows: {format_count(summary['loaded_rows_max'])}")
        lines.append(
            f"  raw: {summary['raw_field_count']} field(s) "
            f"({summary['raw_field_ids'] or 'none'}), "
            f"decoded {format_bytes(raw_total)}, "
            f"serialized {format_bytes(summary['raw_serialized_bytes_total'])}, "
            f"objects {format_count(summary['raw_storage_object_count_total'])}"
        )
        for row in raw_rows:
            stats = object_stats[(segment_id, "raw", row["field_id"])]
            serialized_text = (
                "unknown"
                if stats["unknown_serialized_count"] > 0
                else format_bytes(stats["serialized_bytes"])
            )
            lines.append(
                f"    - {format_field_title(row)}: "
                f"decoded {format_bytes(row['segment_length_bytes'])}, "
                f"serialized {serialized_text}, "
                f"objects {format_count(stats['count'])}, "
                f"load {row['load_mode'] or 'unknown'}"
            )
        lines.append(
            f"  index: {summary['index_field_count']} field(s) "
            f"({summary['index_field_ids'] or 'none'}), "
            f"decoded {format_bytes(index_total)}, "
            f"serialized {format_bytes(summary['index_serialized_bytes_total'])}, "
            f"objects {format_count(summary['index_storage_object_count_total'])}"
        )
        for row in index_rows:
            stats = object_stats[(segment_id, "index", row["field_id"])]
            lines.append(
                f"    - {format_field_title(row)}: "
                f"decoded {format_bytes(row['index_length_bytes'])}, "
                f"serialized {format_bytes(row['serialized_bytes'])}, "
                f"objects {format_count(stats['count'])}, "
                f"load {row['load_mode'] or 'unknown'}, "
                f"mmap {row['mmap']}"
            )
        if summary["index_metadata_object_count"] > 0:
            meta = metadata_stats[segment_id]
            lines.append(
                f"  index metadata: objects {format_count(meta['count'])}, "
                f"serialized {format_bytes(meta['serialized_bytes'])}, "
                f"deserialized {format_bytes(meta['deserialized_bytes'])}"
            )
        lines.append(
            f"  decoded total: {format_bytes(decoded_total)}, "
            f"index/raw ratio: {format_ratio(index_total, raw_total)}"
        )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Parse Milvus lazy-load segment length traces into CSV."
    )
    parser.add_argument("paths", nargs="*", help="Log files to parse. Reads stdin when omitted.")
    parser.add_argument(
        "--text-output",
        help="Write a human-readable text report to this path. Use '-' for stdout.",
    )
    parser.add_argument("--summary-csv", help="Write segment summary CSV to this path.")
    parser.add_argument("--detail-csv", help="Write per-field CSV to this path.")
    parser.add_argument("--object-csv", help="Write per-object CSV to this path.")
    args = parser.parse_args()

    segment_rows = []
    object_rows = []
    segment_summary = defaultdict(lambda: {
        "segment_id": 0,
        "raw_field_count": 0,
        "raw_segment_length_bytes_total": 0,
        "raw_deserialized_bytes_total": 0,
        "raw_storage_object_count_total": 0,
        "index_field_count": 0,
        "index_length_bytes_total": 0,
        "index_serialized_bytes_total": 0,
        "index_deserialized_bytes_total": 0,
        "index_storage_object_count_total": 0,
        "index_metadata_object_count": 0,
        "index_metadata_serialized_bytes_total": 0,
        "index_metadata_deserialized_bytes_total": 0,
        "loaded_rows_max": 0,
        "loaded_rows_min": None,
        "raw_field_ids": [],
        "index_field_ids": [],
    })
    object_serialized_totals = defaultdict(int)
    object_serialized_unknown_counts = defaultdict(int)
    index_object_serialized_totals = defaultdict(int)
    index_object_deserialized_totals = defaultdict(int)

    for line in read_lines(args.paths):
        for event in extract_events_from_line(line):
            if event.get("event") == SEGMENT_EVENT:
                row = normalize_segment_event(event)
                segment_rows.append(row)
                bucket = segment_summary[row["segment_id"]]
                bucket["segment_id"] = row["segment_id"]
                bucket["raw_field_count"] += 1
                bucket["raw_segment_length_bytes_total"] += row["segment_length_bytes"]
                bucket["raw_deserialized_bytes_total"] += row["deserialized_bytes"]
                bucket["raw_storage_object_count_total"] += row["storage_object_count"]
                bucket["loaded_rows_max"] = max(bucket["loaded_rows_max"], row["loaded_rows"])
                if bucket["loaded_rows_min"] is None or row["loaded_rows"] < bucket["loaded_rows_min"]:
                    bucket["loaded_rows_min"] = row["loaded_rows"]
                bucket["raw_field_ids"].append(row["field_id"])
            elif event.get("event") == INDEX_EVENT:
                row = normalize_index_event(event)
                segment_rows.append(row)
                bucket = segment_summary[row["segment_id"]]
                bucket["segment_id"] = row["segment_id"]
                bucket["index_field_count"] += 1
                bucket["index_length_bytes_total"] += row["index_length_bytes"]
                bucket["index_serialized_bytes_total"] += row["serialized_bytes"]
                bucket["index_deserialized_bytes_total"] += row["deserialized_bytes"]
                bucket["index_storage_object_count_total"] += row["storage_object_count"]
                bucket["index_field_ids"].append(row["field_id"])
            elif event.get("event") == OBJECT_EVENT:
                row = normalize_object_event(event)
                object_rows.append(row)
                if (
                    row["data_source"] == "index"
                    and row["included_in_index_length"] == "true"
                ):
                    index_object_serialized_totals[row["segment_id"]] += max(row["serialized_bytes"], 0)
                    index_object_deserialized_totals[row["segment_id"]] += max(row["deserialized_bytes"], 0)
                elif row["data_source"] == "raw":
                    if row["serialized_bytes"] < 0:
                        object_serialized_unknown_counts[row["segment_id"]] += 1
                    else:
                        object_serialized_totals[row["segment_id"]] += row["serialized_bytes"]
                elif row["data_source"] == "index_metadata":
                    bucket = segment_summary[row["segment_id"]]
                    bucket["segment_id"] = row["segment_id"]
                    bucket["index_metadata_object_count"] += 1
                    bucket["index_metadata_serialized_bytes_total"] += max(row["serialized_bytes"], 0)
                    bucket["index_metadata_deserialized_bytes_total"] += max(row["deserialized_bytes"], 0)

    summary_rows = []
    for bucket in segment_summary.values():
        summary_rows.append({
            "segment_id": bucket["segment_id"],
            "raw_field_count": bucket["raw_field_count"],
            "raw_segment_length_bytes_total": bucket["raw_segment_length_bytes_total"],
            "raw_deserialized_bytes_total": bucket["raw_deserialized_bytes_total"],
            "raw_serialized_bytes_total": (
                -1
                if object_serialized_unknown_counts[bucket["segment_id"]] > 0
                else object_serialized_totals[bucket["segment_id"]]
            ),
            "raw_serialized_bytes_unknown_count": object_serialized_unknown_counts[
                bucket["segment_id"]
            ],
            "raw_storage_object_count_total": bucket["raw_storage_object_count_total"],
            "index_field_count": bucket["index_field_count"],
            "index_length_bytes_total": bucket["index_length_bytes_total"],
            "index_deserialized_bytes_total": bucket["index_deserialized_bytes_total"],
            "index_serialized_bytes_total": max(
                bucket["index_serialized_bytes_total"],
                index_object_serialized_totals[bucket["segment_id"]],
            ),
            "index_object_deserialized_bytes_total": index_object_deserialized_totals[bucket["segment_id"]],
            "index_storage_object_count_total": bucket["index_storage_object_count_total"],
            "index_metadata_object_count": bucket["index_metadata_object_count"],
            "index_metadata_serialized_bytes_total": bucket["index_metadata_serialized_bytes_total"],
            "index_metadata_deserialized_bytes_total": bucket["index_metadata_deserialized_bytes_total"],
            "loaded_rows_max": bucket["loaded_rows_max"],
            "loaded_rows_min": bucket["loaded_rows_min"] if bucket["loaded_rows_min"] is not None else 0,
            "raw_field_ids": ";".join(str(v) for v in bucket["raw_field_ids"]),
            "index_field_ids": ";".join(str(v) for v in bucket["index_field_ids"]),
        })

    summary_rows.sort(key=lambda row: row["segment_id"])
    segment_rows.sort(key=lambda row: (row["segment_id"], row["field_id"]))
    object_rows.sort(key=lambda row: (row["segment_id"], row["field_id"], row["object_path"]))

    summary_fields = [
        "segment_id",
        "raw_field_count",
        "raw_segment_length_bytes_total",
        "raw_deserialized_bytes_total",
        "raw_serialized_bytes_total",
        "raw_serialized_bytes_unknown_count",
        "raw_storage_object_count_total",
        "index_field_count",
        "index_length_bytes_total",
        "index_deserialized_bytes_total",
        "index_serialized_bytes_total",
        "index_object_deserialized_bytes_total",
        "index_storage_object_count_total",
        "index_metadata_object_count",
        "index_metadata_serialized_bytes_total",
        "index_metadata_deserialized_bytes_total",
        "loaded_rows_max",
        "loaded_rows_min",
        "raw_field_ids",
        "index_field_ids",
    ]
    detail_fields = [
        "data_source",
        "segment_id",
        "field_id",
        "field_name",
        "data_type",
        "dim",
        "expected_rows",
        "loaded_rows",
        "chunk_count",
        "deserialized_rows",
        "serialized_bytes",
        "deserialized_bytes",
        "column_bytes",
        "segment_length_bytes",
        "load_mode",
        "mmap",
        "mmap_requested",
        "storage_object_count",
        "storage_entries_total",
        "storage_objects",
        "storage_uri",
        "storage_version",
        "index_id",
        "index_build_id",
        "index_version",
        "index_type",
        "index_length_bytes",
    ]
    object_fields = [
        "data_source",
        "segment_id",
        "field_id",
        "index_id",
        "index_build_id",
        "index_version",
        "index_type",
        "object_kind",
        "included_in_index_length",
        "object_path",
        "storage_uri",
        "storage_version",
        "serialized_bytes",
        "deserialized_bytes",
        "deserialized_rows",
        "deserialized_length",
        "deserialized_dim",
        "data_type",
    ]

    emit_text = args.text_output is not None or not any(
        [args.summary_csv, args.detail_csv, args.object_csv]
    )
    if emit_text:
        report = build_text_report(summary_rows, segment_rows, object_rows)
        if args.text_output and args.text_output != "-":
            write_text(args.text_output, report)
        else:
            write_text(None, report)

    write_csv(args.summary_csv, summary_rows, summary_fields)
    if args.detail_csv:
        write_csv(args.detail_csv, segment_rows, detail_fields)
    if args.object_csv:
        write_csv(args.object_csv, object_rows, object_fields)


if __name__ == "__main__":
    main()
