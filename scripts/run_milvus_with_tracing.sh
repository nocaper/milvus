#!/bin/bash
# Start Milvus with latency tracing (Standalone Mode)
#
# Latency events are exported asynchronously to a dedicated file so that
# tracing does not serialize the request path on stdout.
#
# Usage:
#   ./scripts/run_milvus_with_tracing.sh

# Check if Milvus binary exists
if [ ! -f "./bin/milvus" ]; then
    echo "❌ Error: ./bin/milvus not found"
    echo "   Please compile Milvus first: make milvus"
    exit 1
fi

# Check if Milvus is already running
if pgrep -f "milvus run standalone" > /dev/null; then
    echo "⚠️  Milvus is already running!"
    echo "   To restart, first stop it:"
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

LOG_FILE="${MILVUS_LOG_FILE:-/tmp/milvus_standalone.log}"
TRACE_FILE="${MILVUS_LATENCY_TRACE_OUTPUT:-/tmp/milvus_traces/latency_trace.jsonl}"

if [[ "$TRACE_FILE" == "stdout" || "$TRACE_FILE" == "stderr" ]]; then
    echo "Error: MILVUS_LATENCY_TRACE_OUTPUT must be a file path for this script"
    exit 1
fi

export MILVUS_LATENCY_TRACE_ENABLED="${MILVUS_LATENCY_TRACE_ENABLED:-true}"
export MILVUS_LATENCY_TRACE_OUTPUT="$TRACE_FILE"
mkdir -p "$(dirname "$TRACE_FILE")"
: > "$TRACE_FILE"

echo "=========================================="
echo "Starting Milvus Standalone with Latency Tracing"
echo "=========================================="
echo "Milvus log file: $LOG_FILE"
echo "Latency trace file: $TRACE_FILE"
echo ""

# Keep normal Milvus logs separate from latency trace events.
nohup ./bin/milvus run standalone > "$LOG_FILE" 2>&1 &
MILVUS_PID=$!

echo "✓ Milvus started with PID: $MILVUS_PID"
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
echo "Next steps:"
echo "1. Run your benchmark or demo:"
echo "   python scripts/demo_latency_tracing.py"
echo ""
echo "2. Analyze the latency trace file:"
echo "   python scripts/analyze_latency_traces.py \\"
echo "       $TRACE_FILE \\"
echo "       -o ./latency_report"
echo ""
echo "   Or pipe a live tail into the analyzer:"
echo "   tail -n +1 -f $TRACE_FILE | python scripts/analyze_latency_traces.py -"
echo ""
echo "3. View results:"
echo "   ls latency_report/"
echo ""
echo "To stop Milvus:"
echo "  kill $MILVUS_PID"
echo "  # or"
echo "  pkill -f 'milvus run standalone'"
echo ""
echo "To follow the log:"
echo "  tail -f $LOG_FILE"
echo ""
echo "To follow latency traces:"
echo "  tail -f $TRACE_FILE"
