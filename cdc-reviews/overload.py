"""변경량과 컨슈머 수를 바꾸며 지연과 처리량을 측정한다."""

import json
import threading
import time

import daily_check
import simulate
from common import LOG_DIR
from run_scenarios import (
    lag,
    max_event_id,
    start_consumer,
    stats,
    verify,
    wait_drained,
)

DURATION = 60
OUT = {}


def run_load(name, rate, n_consumers):
    labels = [f"{name}_c{i + 1}" for i in range(n_consumers)]
    procs = [start_consumer(lb) for lb in labels]
    time.sleep(8)  # 그룹 합류와 파티션 배정 대기
    pool = simulate.pick(rate * DURATION)
    since = max_event_id()

    samples, stop = [], threading.Event()
    t0 = time.time()

    def sampler():
        while not stop.is_set():
            samples.append((round(time.time() - t0, 1), lag()[0]))
            time.sleep(0.5)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    per_tick = max(1, rate // 10)
    entries, i = [], 0
    while i < len(pool):
        tick = time.time()
        entries += simulate.update(name, pool[i : i + per_tick], name, chunk=per_tick)
        i += per_tick
        rest = 0.1 - (time.time() - tick)
        if rest > 0:
            time.sleep(rest)
    t_written = time.time()
    wait_drained(timeout=900)
    t_drained = time.time()
    stop.set()
    th.join()

    v = verify(entries, since)
    per_consumer = {lb: stats(lb).get("processed", 0) for lb in labels}
    total_s = t_drained - t0
    result = {
        "target_rate": rate,
        "consumers": n_consumers,
        "write_s": round(t_written - t0, 1),
        "achieved_write_rate": round(len(entries) / (t_written - t0)),
        "max_lag": max(s[1] for s in samples),
        "lag_at_write_end": next((s[1] for s in reversed(samples) if s[0] <= t_written - t0), None),
        "drain_after_write_s": round(t_drained - t_written, 1),
        "throughput_per_s": round(len(entries) / total_s),
        "processed_per_consumer": per_consumer,
        **v,
        "lag_series": samples,
    }
    for p in procs:
        p.poll()
    stop_consumer_all(procs)
    OUT[name] = result
    print(
        name,
        json.dumps({k: v for k, v in result.items() if k != "lag_series"}, ensure_ascii=False),
        flush=True,
    )


def stop_consumer_all(procs):
    import os

    open((LOG_DIR / "STOP"), "w").close()
    for p in procs:
        p.wait(timeout=120)
    os.remove((LOG_DIR / "STOP"))


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for rate in (300, 600, 1200):
        run_load(f"O1_rate{rate}", rate, 1)
    for n in (1, 2, 3, 4):
        run_load(f"O2_c{n}", 1200, n)
    OUT["final_check"] = daily_check.run("overload_final")
    print("final_check", OUT["final_check"], flush=True)
    with open((LOG_DIR / "overload.json"), "w", encoding="utf-8") as f:
        json.dump(OUT, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
