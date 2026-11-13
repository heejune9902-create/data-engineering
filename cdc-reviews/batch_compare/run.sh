#!/usr/bin/env bash
# 같은 변경 부하에서 CDC와 배치 수집을 비교한다.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python}"
mkdir -p logs batch_compare/logs
if [[ -e logs/STOP ]]; then rm -f logs/STOP; fi
"$PY" -c "from common import dw; c=dw(); c.autocommit=True; c.cursor().execute('CREATE SCHEMA IF NOT EXISTS bexp')"
WARM=60; LOAD=300; TAIL=80
TOTAL=$((WARM + LOAD + TAIL))
pids=()
cleanup() {
    touch logs/STOP
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    done
    rm -f logs/STOP
}
trap cleanup EXIT

"$PY" batch_compare/sampler.py --duration $((TOTAL + 10)) &
pids+=("$!")
"$PY" consumer.py --label cmp > batch_compare/logs/consumer.out 2>&1 &
pids+=("$!")
"$PY" batch_compare/batch_loader.py --mode inc --interval 60 --table inc60 --duration "$TOTAL" > batch_compare/logs/inc60.out 2>&1 &
pids+=("$!")
"$PY" batch_compare/batch_loader.py --mode inc --interval 10 --table inc10 --duration "$TOTAL" > batch_compare/logs/inc10.out 2>&1 &
pids+=("$!")
"$PY" batch_compare/batch_loader.py --mode full --interval 60 --table full60 --duration "$TOTAL" > batch_compare/logs/full60.out 2>&1 &
pids+=("$!")

date +%s > batch_compare/logs/t_start
sleep "$WARM"
date +%s > batch_compare/logs/t_load_start
"$PY" batch_compare/workload.py --duration "$LOAD" --rate 5
date +%s > batch_compare/logs/t_load_end
sleep "$TAIL"
touch logs/STOP
status=0
for pid in "${pids[@]}"; do wait "$pid" || status=1; done
if [[ "$status" != 0 ]]; then
    echo "======= [ERROR] 비교 작업 실패: 로그를 확인하세요 =======" >&2
    exit "$status"
fi
echo "[INFO] 비교 작업 완료"
