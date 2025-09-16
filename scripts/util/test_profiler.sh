#!/usr/bin/env bash
set -euo pipefail

# 1) wait for the Runner to start
echo "Waiting for org.scalatest.tools.Runner to appear…"
while true; do
  PID=$(pgrep -f 'org\.scalatest\.tools\.Runner' | head -n1 || true)
  if [[ -n "$PID" ]]; then
    echo "Found Runner PID=$PID"
    break
  fi
  sleep 1
done

# 2) prepare output paths
TS=$(date +%Y%m%d_%H%M%S)
OUT_HTML="/tmp/scala_test_profile_${TS}.html"
OUT_SUM="/tmp/scala_test_summary_${TS}.txt"

echo "Profiling for the lifetime of PID=$PID…"
# 3) no -d => runs until the target JVM exits
/tmp/profiler.sh \
  -d 210 \
  -i 10ms \
  -e cpu \
  -o flat,tree,flamegraph \
  -f "${OUT_HTML}" \
  "${PID}" > "${OUT_SUM}"

echo "Profiler done. Results:"
echo " • Flamegraph → ${OUT_HTML}"
echo " • Summary    → ${OUT_SUM}"
