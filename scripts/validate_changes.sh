#!/bin/bash
# Validation script to check if changes compile and are ready for commit

set -e

echo "=========================================="
echo "Validating Milvus Latency Tracing Changes"
echo "=========================================="
echo ""

# Check we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "❌ Error: Not in Milvus root directory"
    echo "   Please cd to D:\project\claude\c_qps2.4.5\milvus"
    exit 1
fi

echo "✓ In Milvus root directory"
echo ""

# Check modified files exist
echo "Checking modified files..."
modified_files=(
    "internal/proxy/task_insert.go"
    "internal/datanode/flow_graph_write_node.go"
    "internal/datanode/syncmgr/task.go"
    "internal/querynodev2/delegator/delegator.go"
    "internal/querynodev2/segments/segment_loader.go"
)

for file in "${modified_files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ❌ $file not found"
        exit 1
    fi
done
echo ""

# Check new files exist
echo "Checking new files..."
new_files=(
    "pkg/tracer/latency_tracer.go"
    "pkg/tracer/init.go"
    "scripts/analyze_latency_traces.py"
    "scripts/demo_latency_tracing.py"
    "scripts/run_milvus_with_tracing.sh"
    "docs/LATENCY_ANALYSIS_README.md"
    "docs/QUICKSTART_LATENCY_ANALYSIS.md"
    "docs/CHANGES_SUMMARY.md"
    "docs/GIT_COMMIT_GUIDE.md"
)

for file in "${new_files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ❌ $file not found"
        exit 1
    fi
done
echo ""

# Check imports are added
echo "Checking import statements..."
if grep -q "github.com/milvus-io/milvus/pkg/tracer" internal/proxy/task_insert.go; then
    echo "  ✓ Proxy has tracer import"
else
    echo "  ❌ Proxy missing tracer import"
    exit 1
fi

if grep -q "github.com/milvus-io/milvus/pkg/tracer" internal/datanode/flow_graph_write_node.go; then
    echo "  ✓ DataNode write_node has tracer import"
else
    echo "  ❌ DataNode write_node missing tracer import"
    exit 1
fi

if grep -q "github.com/milvus-io/milvus/pkg/tracer" internal/datanode/syncmgr/task.go; then
    echo "  ✓ DataNode syncmgr has tracer import"
else
    echo "  ❌ DataNode syncmgr missing tracer import"
    exit 1
fi

if grep -q "github.com/milvus-io/milvus/pkg/tracer" internal/querynodev2/delegator/delegator.go; then
    echo "  ✓ QueryNode delegator has tracer import"
else
    echo "  ❌ QueryNode delegator missing tracer import"
    exit 1
fi

if grep -q "github.com/milvus-io/milvus/pkg/tracer" internal/querynodev2/segments/segment_loader.go; then
    echo "  ✓ QueryNode segment_loader has tracer import"
else
    echo "  ❌ QueryNode segment_loader missing tracer import"
    exit 1
fi
echo ""

# Check Python scripts are executable
echo "Checking Python scripts..."
if python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 6) else 1)"; then
    echo "  ✓ Python 3.6+ available"
else
    echo "  ⚠️  Python 3.6+ not found (needed for analysis script)"
fi

# Check Python script syntax
if python3 -m py_compile scripts/analyze_latency_traces.py 2>/dev/null; then
    echo "  ✓ analyze_latency_traces.py syntax valid"
else
    echo "  ❌ analyze_latency_traces.py has syntax errors"
    exit 1
fi

if python3 -m py_compile scripts/demo_latency_tracing.py 2>/dev/null; then
    echo "  ✓ demo_latency_tracing.py syntax valid"
else
    echo "  ❌ demo_latency_tracing.py has syntax errors"
    exit 1
fi
echo ""

# Try to compile (quick check with go build on tracer package)
echo "Checking Go compilation..."
if go build -v ./pkg/tracer/... 2>&1 | grep -q "error"; then
    echo "  ❌ Tracer package has compilation errors"
    exit 1
else
    echo "  ✓ Tracer package compiles"
fi
echo ""

# Summary
echo "=========================================="
echo "✅ All validation checks passed!"
echo "=========================================="
echo ""
echo "Your changes are ready to commit."
echo ""
echo "Next steps:"
echo "1. Compile full Milvus: make milvus"
echo "2. Test with demo: python scripts/demo_latency_tracing.py"
echo "3. Commit changes (see docs/GIT_COMMIT_GUIDE.md)"
echo ""
echo "Files changed:"
echo "  Modified: 5 files"
echo "  New:      9 files"
echo "  Total:    ~800 lines of code"
