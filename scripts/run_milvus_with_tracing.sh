#!/bin/bash
# Start Milvus with latency tracing enabled (Standalone Mode)
#
# Usage:
#   ./scripts/run_milvus_with_tracing.sh
#
# This script sets environment variables to enable latency tracing
# and starts Milvus standalone

# Enable latency tracing
export MILVUS_LATENCY_TRACE_ENABLED=true

# Set output directory for trace files
TRACE_DIR="/tmp/milvus_traces"
mkdir -p "$TRACE_DIR"

export MILVUS_LATENCY_TRACE_OUTPUT="$TRACE_DIR/latency_trace.jsonl"

echo "=========================================="
echo "Starting Milvus Standalone with Latency Tracing"
echo "=========================================="
echo "Trace output: $MILVUS_LATENCY_TRACE_OUTPUT"
echo ""

# Check if Milvus binary exists
if [ ! -f "./bin/milvus" ]; then
    echo "❌ Error: ./bin/milvus not found"
    echo "   Please compile Milvus first: make milvus"
    exit 1
fi

# Check if Milvus is already running
if pgrep -f "milvus run standalone" > /dev/null; then
    echo "⚠️  Milvus is already running!"
    echo "   To restart with tracing, first stop it:"
    echo "   pkill -f 'milvus run standalone'"
    echo ""
    read -p "Stop existing Milvus and restart? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping existing Milvus..."
        pkill -f "milvus run standalone"
        sleep 2
    else
        echo "Exiting without changes."
        exit 0
    fi
fi

# Start Milvus standalone
echo "Starting Milvus standalone..."
LOG_FILE="/tmp/milvus_standalone.log"

# Clear old trace file
> "$MILVUS_LATENCY_TRACE_OUTPUT"
echo "Cleared old trace file"

# Start Milvus in background
nohup ./bin/milvus run standalone > "$LOG_FILE" 2>&1 &
MILVUS_PID=$!

echo "✓ Milvus started with PID: $MILVUS_PID"
echo "  Log file: $LOG_FILE"
echo ""

# Wait for Milvus to start
echo "Waiting for Milvus to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:9091/healthz > /dev/null 2>&1; then
        echo "✓ Milvus is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for Milvus to start"
        echo "   Check logs: tail -f $LOG_FILE"
        exit 1
    fi
    echo -n "."
    sleep 1
done
echo ""

echo ""
echo "=========================================="
echo "Milvus is running with tracing enabled!"
echo "=========================================="
echo ""
echo "Environment variables set:"
echo "  MILVUS_LATENCY_TRACE_ENABLED=true"
echo "  MILVUS_LATENCY_TRACE_OUTPUT=$MILVUS_LATENCY_TRACE_OUTPUT"
echo ""
echo "Next steps:"
echo "1. Run your benchmark or demo:"
echo "   python scripts/demo_latency_tracing.py"
echo ""
echo "2. Analyze traces:"
echo "   python scripts/analyze_latency_traces.py \\"
echo "       $MILVUS_LATENCY_TRACE_OUTPUT \\"
echo "       -o ./latency_report"
echo ""
echo "3. View results:"
echo "   ls latency_report/"
echo ""
echo "To stop Milvus:"
echo "  kill $MILVUS_PID"
echo "  # or"
echo "  pkill -f 'milvus run standalone'"
echo ""
echo "To view logs:"
echo "  tail -f $LOG_FILE"

