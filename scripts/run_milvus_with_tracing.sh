#!/bin/bash
# Start Milvus with latency tracing (Standalone Mode)
#
# Latency tracing is always enabled in the binary (hardcoded).
# This script starts Milvus and captures stdout to a log file so that
# [LATENCY_TRACE] lines can be parsed by analyze_latency_traces.py.
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

LOG_FILE="/tmp/milvus_standalone.log"

echo "=========================================="
echo "Starting Milvus Standalone with Latency Tracing"
echo "=========================================="
echo "Log file (contains [LATENCY_TRACE] lines): $LOG_FILE"
echo ""

# Start Milvus; pipe stdout+stderr to tee so the log is captured and also
# visible on the terminal when running interactively.
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
echo "2. Analyze traces from the log:"
echo "   python scripts/analyze_latency_traces.py \\"
echo "       $LOG_FILE \\"
echo "       -o ./latency_report"
echo ""
echo "   Or pipe a live tail into the analyzer:"
echo "   tail -n +1 -f $LOG_FILE | python scripts/analyze_latency_traces.py -"
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
