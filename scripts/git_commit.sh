#!/bin/bash
# Git commit script for latency tracing changes

set -e

echo "=========================================="
echo "Preparing Git Commit"
echo "=========================================="
echo ""

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "❌ Error: Not in a git repository"
    exit 1
fi

# Show git status
echo "Current git status:"
git status --short
echo ""

# Show changed files count
MODIFIED_COUNT=$(git status --short | grep "^ M" | wc -l)
NEW_COUNT=$(git status --short | grep "^??" | wc -l)
echo "Modified files: $MODIFIED_COUNT"
echo "New files: $NEW_COUNT"
echo ""

# Add all changes
echo "Adding all changes..."
git add -A

# Show what will be committed
echo ""
echo "Files to be committed:"
git status --short
echo ""

# Commit message
COMMIT_MSG="feat: Add latency tracing for bottleneck analysis and shared memory pool optimization modeling

This commit adds fine-grained latency instrumentation to Milvus 2.4.5 for
quantitative performance bottleneck analysis, specifically to model the expected
benefits of a shared memory pool optimization between DataNode and QueryNode.

## Motivation

To optimize Milvus by implementing a shared memory pool that allows QueryNode to
access segment data directly from DataNode (avoiding S3 read and deserialization),
we need to:
1. Quantify the current segment load latency from S3 (primary optimization target)
2. Measure the distribution of growing vs sealed segment queries (applicability)
3. Model the expected QPS improvement and latency reduction

## Changes

### Modified Files (5)
- internal/proxy/task_insert.go
  - Generate trace_id for each Insert request
  - Add instrumentation: serialize, mq_produce stages

- internal/datanode/flow_graph_write_node.go
  - Add instrumentation: consume_lag, datanode_process stages

- internal/datanode/syncmgr/task.go
  - Add instrumentation: s3_write stage

- internal/querynodev2/delegator/delegator.go
  - Generate trace_id for each Search request
  - Add instrumentation: route, segment_stats stages

- internal/querynodev2/segments/segment_loader.go
  - Add instrumentation: total_load stage (PRIMARY OPTIMIZATION TARGET)

### New Files (11)
- pkg/tracer/latency_tracer.go: Core tracing framework
- pkg/tracer/init.go: Initialization from environment variables
- scripts/analyze_latency_traces.py: Analysis script with visualization
- scripts/demo_latency_tracing.py: Demo script for validation
- scripts/run_milvus_with_tracing.sh: Startup script (bare-metal standalone)
- scripts/validate_changes.sh: Validation script
- docs/LATENCY_ANALYSIS_README.md: Detailed technical documentation
- docs/QUICKSTART_LATENCY_ANALYSIS.md: Quick start guide
- docs/CHANGES_SUMMARY.md: Summary of changes and conclusions
- docs/STANDALONE_DEPLOYMENT_CHECKLIST.md: Bare-metal deployment checklist
- docs/STANDALONE_VERIFICATION_REPORT.md: Verification report
- README_LATENCY_TRACING.md: Main README

## Tracing Architecture

Write Path (Insert):
  Proxy: serialize → mq_produce
  DataNode: consume_lag → datanode_process → s3_write

Read Path (Search):
  QueryNode: route → segment_stats
  If sealed segment not loaded:
    QueryNode: total_load (from S3) ← PRIMARY OPTIMIZATION TARGET

## Deployment

Designed for bare-metal standalone deployment:

\`\`\`bash
# Enable tracing
export MILVUS_LATENCY_TRACE_ENABLED=true
export MILVUS_LATENCY_TRACE_OUTPUT=/tmp/milvus_traces/latency_trace.jsonl

# Start Milvus
./bin/milvus run standalone

# Or use provided script
./scripts/run_milvus_with_tracing.sh

# Analyze results
python scripts/analyze_latency_traces.py /tmp/milvus_traces/latency_trace.jsonl -o report
\`\`\`

## Expected Insights

The analysis will quantify:
- Segment load latency from S3 (e.g., 486ms mean)
- Frequency of segment loads (e.g., 125 times)
- Growing vs sealed segment query distribution (e.g., 35% / 65%)
- Expected optimization impact (e.g., 50x throughput increase)

This data-driven approach enables informed decision-making on whether
to implement the shared memory pool optimization.

## Key Findings

After running benchmarks, you can answer:
1. Is segment load from S3 a bottleneck? (quantified latency)
2. What percentage of queries would benefit? (sealed vs growing ratio)
3. What is the expected QPS improvement? (calculated from latency reduction)
4. Is the optimization worth implementing? (data-driven conclusion)

## Limitations

- Trace ID propagation through MQ not yet implemented (Proxy and DataNode
  traces are independent)
- Tracer initialization relies on environment variables
- Total_load is a single metric (does not break down S3 read vs deserialization)

These limitations do not affect the core functionality of bottleneck quantification.

## Testing

- Code compiles successfully with \`make milvus\`
- Demo script validates tracing functionality
- Analysis script generates reports and visualizations
- All scripts optimized for bare-metal standalone deployment

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"

# Create commit
echo "Creating commit..."
git commit -m "$COMMIT_MSG"

echo ""
echo "=========================================="
echo "✅ Commit created successfully!"
echo "=========================================="
echo ""
echo "Commit details:"
git log -1 --stat
echo ""
echo "Next steps:"
echo "1. Review the commit: git show HEAD"
echo "2. Push to remote: git push origin c_qps2.4.5"
echo "3. Create PR if needed"
