#!/usr/bin/env python3
"""
Quick demo script to validate latency tracing instrumentation

This script:
1. Connects to Milvus
2. Creates a collection
3. Inserts data
4. Performs searches
5. Shows where trace data should be collected

Run this after starting Milvus with tracing enabled.
"""

import time
import os
import numpy as np
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType

def main():
    trace_file = os.environ.get(
        "MILVUS_LATENCY_TRACE_OUTPUT",
        "/tmp/milvus_traces/latency_trace.jsonl",
    )

    print("="*60)
    print("Milvus Latency Tracing Demo")
    print("="*60)

    # Connect to Milvus
    print("\n1. Connecting to Milvus...")
    connections.connect(host="localhost", port="19530")
    print("   ✓ Connected")

    # Create collection
    print("\n2. Creating test collection...")
    collection_name = "latency_trace_test"

    # Drop if exists
    try:
        Collection(collection_name).drop()
    except:
        pass

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=128)
    ]
    schema = CollectionSchema(fields=fields, description="Test collection for latency tracing")
    collection = Collection(name=collection_name, schema=schema)
    print(f"   ✓ Collection '{collection_name}' created")

    # Insert data
    print("\n3. Inserting data...")
    num_entities = 10000
    ids = list(range(num_entities))
    embeddings = np.random.rand(num_entities, 128).tolist()

    insert_start = time.time()
    collection.insert([ids, embeddings])
    insert_time = time.time() - insert_start
    print(f"   ✓ Inserted {num_entities} entities in {insert_time:.2f}s")
    print(f"   → Check proxy trace: serialize + mq_produce stages")

    # Flush to trigger DataNode -> S3 write
    print("\n4. Flushing data (triggers DataNode -> S3 write)...")
    flush_start = time.time()
    collection.flush()
    flush_time = time.time() - flush_start
    print(f"   ✓ Flush completed in {flush_time:.2f}s")
    print(f"   → Check datanode trace: consume_lag + datanode_process + s3_write stages")

    # Create index
    print("\n5. Creating index...")
    index_params = {
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128}
    }
    collection.create_index(field_name="embedding", index_params=index_params)
    print("   ✓ Index created")

    # Load collection (triggers QueryNode segment load from S3)
    print("\n6. Loading collection (triggers segment load from S3)...")
    load_start = time.time()
    collection.load()
    load_time = time.time() - load_start
    print(f"   ✓ Collection loaded in {load_time:.2f}s")
    print(f"   → Check querynode trace: total_load stage (THIS IS THE KEY METRIC!)")
    print(f"   → This latency ({load_time:.2f}s) is what shared memory pool will eliminate")

    # Perform searches
    print("\n7. Performing searches...")
    search_vectors = np.random.rand(10, 128).tolist()
    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

    search_start = time.time()
    results = collection.search(
        data=search_vectors,
        anns_field="embedding",
        param=search_params,
        limit=10
    )
    search_time = time.time() - search_start
    print(f"   ✓ Completed 10 searches in {search_time:.2f}s")
    print(f"   → Check querynode trace: route + segment_stats stages")
    print(f"   → Look for sealed_segments count (indicates optimization potential)")

    # Insert more data to create growing segments
    print("\n8. Inserting more data (creates growing segments)...")
    new_ids = list(range(num_entities, num_entities + 1000))
    new_embeddings = np.random.rand(1000, 128).tolist()
    collection.insert([new_ids, new_embeddings])
    time.sleep(1)  # Wait for growing segment to be visible

    # Search again to hit growing segments
    print("\n9. Searching again (should hit growing segments)...")
    search_start = time.time()
    results = collection.search(
        data=search_vectors,
        anns_field="embedding",
        param=search_params,
        limit=10
    )
    search_time = time.time() - search_start
    print(f"   ✓ Completed searches in {search_time:.2f}s")
    print(f"   → Check querynode trace: growing_segments count should be > 0")
    print(f"   → Growing segments are already fast (no S3 load needed)")

    # Cleanup
    print("\n10. Cleanup...")
    collection.drop()
    print("   ✓ Collection dropped")

    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)
    print("\nNext steps:")
    print(f"1. Check trace file: {trace_file}")
    print("2. Run analysis:")
    print("   python scripts/analyze_latency_traces.py \\")
    print(f"       {trace_file} \\")
    print("       -o ./latency_report")
    print("\n3. Review the report to see:")
    print("   - Segment load latency (optimization target)")
    print("   - Write path breakdown")
    print("   - Growing vs Sealed segment distribution")
    print("\nKey metrics to look for:")
    print(f"   • Load collection time: {load_time:.2f}s → target: ~0s with shared memory")
    print(f"   • Expected QPS improvement: significant if load happens frequently")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("1. Milvus is running with tracing enabled")
        print("2. pymilvus is installed: pip install pymilvus")
        print("3. You can connect to localhost:19530")
