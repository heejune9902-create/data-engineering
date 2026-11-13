"""정상 처리, 재시작, 장애와 순서 역전 시나리오를 검증한다."""

import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

import daily_check
import recover_dlt
import simulate
from common import BOOTSTRAP, GROUP, LOG_DIR, PROJECT_ROOT, TOPIC, dw
from confluent_kafka import Consumer, Producer, TopicPartition

PY = sys.executable
RESULTS = {}
_admin = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "enable.auto.commit": False})


def lag():
    md = _admin.list_topics(TOPIC, timeout=10)
    parts = [TopicPartition(TOPIC, p) for p in md.topics[TOPIC].partitions]
    total_lag, total_hi = 0, 0
    for tp in _admin.committed(parts, timeout=10):
        lo, hi = _admin.get_watermark_offsets(tp, timeout=10)
        total_hi += hi
        total_lag += hi - (tp.offset if tp.offset >= 0 else lo)
    return total_lag, total_hi


def wait_drained(timeout=600):
    """Kafka에 새 이벤트가 더 들어오지 않고 컨슈머가 다 읽을 때까지 기다린다."""
    t0, stable, last_hi = time.time(), 0, None
    while time.time() - t0 < timeout:
        lg, hi = lag()
        stable = stable + 1 if (lg == 0 and hi == last_hi) else 0
        last_hi = hi
        if stable >= 3:
            return time.time() - t0
        time.sleep(1)
    raise TimeoutError("pipeline did not drain")


def start_consumer(label, *extra):
    if os.path.exists((LOG_DIR / "STOP")):
        os.remove((LOG_DIR / "STOP"))
    log = open((LOG_DIR / f"consumer_{label}.log"), "w", encoding="utf-8")
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    return subprocess.Popen(
        [PY, str(PROJECT_ROOT / "consumer.py"), "--label", label, *extra],
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
    )


def stop_consumer(p):
    open((LOG_DIR / "STOP"), "w").close()
    p.wait(timeout=60)
    os.remove((LOG_DIR / "STOP"))


def max_event_id():
    with dw() as c, c.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(event_id), 0) FROM f_review_event")
        return cur.fetchone()[0]


def stats(label):
    with dw() as c, c.cursor() as cur:
        cur.execute(
            "SELECT processed, applied, dup_events, stale_skipped, dropped, sent_to_dlt FROM consumer_stats WHERE run_label=%s ORDER BY id DESC LIMIT 1",
            (label,),
        )
        r = cur.fetchone()
    return (
        dict(
            zip(
                ["processed", "applied", "dup_events", "stale_skipped", "dropped", "sent_to_dlt"], r
            )
        )
        if r
        else {}
    )


def all_truth():
    last = {}
    with open(simulate.TRUTH, encoding="utf-8") as f:
        for line in f:
            e = json.loads(line)
            last[e["review_id"]] = e
    return last


def verify(entries, since):
    """정답 로그의 변경이 이벤트 단계와 최종 f_review 단계에서 각각 잡혔는지 센다."""
    ids = list({e["review_id"] for e in entries})
    with dw() as c, c.cursor() as cur:
        cur.execute(
            """SELECT review_id, op, new_hash, EXTRACT(EPOCH FROM (ingested_at - source_ts)) * 1000
                       FROM f_review_event WHERE event_id > %s AND kafka_partition >= 0 AND review_id = ANY(%s)""",
            (since, ids),
        )
        ev = cur.fetchall()
        cur.execute(
            "SELECT review_id, content_hash, is_deleted FROM f_review WHERE review_id = ANY(%s)",
            (ids,),
        )
        fr = {r[0]: r for r in cur.fetchall()}
    seen = {(r[0], r[1], r[2] if r[1] != "d" else None) for r in ev}
    missing_event = sum(1 for e in entries if (e["review_id"], e["op"], e["hash"]) not in seen)
    last = all_truth()
    final_bad = 0
    for rid in ids:
        exp, got = last[rid], fr.get(rid)
        if exp["op"] == "d":
            final_bad += 0 if (got and got[2]) else 1
        else:
            final_bad += 0 if (got and not got[2] and got[1] == exp["hash"]) else 1
    lat = sorted(r[3] for r in ev if r[3] is not None)
    pct = (
        (lambda q: round(float(lat[min(len(lat) - 1, int(q * len(lat)))]), 1))
        if lat
        else (lambda q: None)
    )
    return {
        "truth_changes": len(entries),
        "missing_at_event": missing_event,
        "final_mismatch": final_bad,
        "latency_ms_p50": pct(0.5),
        "latency_ms_p95": pct(0.95),
    }


def produce_raw(pairs):
    p = Producer({"bootstrap.servers": BOOTSTRAP, "partitioner": "murmur2_random", "acks": "all"})
    for k, v in pairs:
        p.produce(TOPIC, key=k, value=v)
    p.flush(10)


def key_of(rid):
    return json.dumps({"review_id": rid}, separators=(",", ":"))


def connect_status():
    try:
        with urllib.request.urlopen(
            os.environ.get("CONNECT_URL", "http://localhost:8083").rstrip("/")
            + "/connectors/reviews-cdc/status",
            timeout=5,
        ) as r:
            s = json.loads(r.read())
        return s["connector"]["state"], s["tasks"][0]["state"] if s["tasks"] else None
    except Exception:  # noqa: BLE001
        return None, None


def record(name, **kw):
    RESULTS[name] = kw
    print(
        f"======= [CHECK] {name} =======",
        json.dumps(kw, ensure_ascii=False, default=str),
        flush=True,
    )


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if os.path.exists(simulate.TRUTH):
        os.remove(simulate.TRUTH)

    record("S0_baseline", **daily_check.run("S0_baseline", do_repair=False))

    # S1 정상
    cons = start_consumer("S1")
    since = max_event_id()
    t0 = time.time()
    e = simulate.update("S1", simulate.pick(500), "S1 수정", pause=0.2)
    e += simulate.delete("S1", simulate.pick(100), pause=0.2)
    e += simulate.insert("S1", 200, pause=0.2)
    wait_drained()
    record(
        "S1_normal", elapsed_s=round(time.time() - t0, 1), **verify(e, since), consumer=stats("S1")
    )

    # S2 컨슈머 강제 종료
    stop_consumer(cons)
    cons = start_consumer("S2_killed", "--commit-every", "5")
    since = max_event_id()
    e = simulate.update("S2", simulate.pick(3000), "S2 수정", chunk=500)
    t_kill = time.time()
    while stats("S2_killed").get("processed", 0) < 1500 and time.time() - t_kill < 60:
        time.sleep(0.02)
    cons.kill()
    cons.wait()
    killed_stats = stats("S2_killed")
    cons = start_consumer("S2_restart")
    wait_drained()
    record(
        "S2_consumer_kill",
        **verify(e, since),
        killed_run=killed_stats,
        restarted_run=stats("S2_restart"),
    )

    # S3 몰림 (세일): 2만 건을 쉬지 않고 수정
    since = max_event_id()
    samples, stop = [], threading.Event()

    def sampler():
        while not stop.is_set():
            samples.append((time.time(), lag()[0]))
            time.sleep(0.5)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    t0 = time.time()
    e = simulate.update("S3", simulate.pick(20000), "S3 세일", chunk=500)
    t_written = time.time()
    drain = wait_drained()
    stop.set()
    th.join()
    record(
        "S3_burst",
        write_s=round(t_written - t0, 1),
        drain_after_write_s=round(time.time() - t_written, 1),
        max_lag=max(s[1] for s in samples),
        **verify(e, since),
    )

    # S4 CDC 중단: Debezium을 멈춘 동안 변경, 재시작 후 이어서 받는지
    since = max_event_id()
    subprocess.run(["docker", "stop", "demo-connect"], check=True, capture_output=True)
    t_down = time.time()
    e = simulate.update("S4", simulate.pick(1000), "S4 중단 중 수정", pause=0.5)
    e += simulate.delete("S4", simulate.pick(100))
    while time.time() - t_down < 60:
        time.sleep(1)
    subprocess.run(["docker", "start", "demo-connect"], check=True, capture_output=True)
    t_up = time.time()
    while connect_status() != ("RUNNING", "RUNNING"):
        time.sleep(2)
        if time.time() - t_up > 300:
            raise TimeoutError("connect did not come back")
    wait_drained()
    record(
        "S4_cdc_down",
        downtime_s=round(t_up - t_down, 1),
        recovery_after_restart_s=round(time.time() - t_up, 1),
        **verify(e, since),
    )

    # S5 처리 실패: 특정 리뷰 처리를 실패시키고 깨진 메시지를 섞는다
    since = max_event_id()
    fail = simulate.pick(20)
    with open((LOG_DIR / "fail_ids.txt"), "w") as f:
        f.write("\n".join(map(str, fail)))
    e = simulate.update("S5", fail + simulate.pick(180, exclude=fail), "S5 수정")
    garbage_ids = simulate.pick(5)
    produce_raw([(key_of(r), b'{"broken": ') for r in garbage_ids])
    wait_drained()
    before = verify(e, since)
    os.remove((LOG_DIR / "fail_ids.txt"))
    rec = recover_dlt.run()
    after = verify(e, since)
    record(
        "S5_failures",
        before_recovery=before,
        dlt_recovery=rec,
        after_recovery_final_mismatch=after["final_mismatch"],
        consumer=stats("S2_restart"),
    )

    # S6 이벤트 유실: 컨슈머가 1%를 건너뛰는 버그
    stop_consumer(cons)
    cons = start_consumer("S6_drop", "--drop-rate", "0.01")
    since = max_event_id()
    e = simulate.update("S6", simulate.pick(5000), "S6 수정", chunk=250, pause=0.05)
    e += simulate.delete("S6", simulate.pick(300))
    wait_drained()
    before = verify(e, since)
    dq = daily_check.run("S6_daily")
    after = verify(e, since)
    record(
        "S6_drop",
        before_check=before,
        consumer=stats("S6_drop"),
        daily_check=dq,
        after_check_final_mismatch=after["final_mismatch"],
    )
    stop_consumer(cons)
    cons = start_consumer("S7")

    # S7 순서 뒤바뀜: 옛 수정 이벤트를 다시 보낸다
    ids = simulate.pick(100)
    since = max_event_id()
    e = simulate.update("S7", ids, "S7 v1")
    wait_drained()
    v1 = simulate.fetch_rows(ids)
    e += simulate.update("S7", ids, "S7 v2")
    wait_drained()
    stale_before = stats("S7").get("stale_skipped", 0)
    fmt = lambda t: t.astimezone(__import__("datetime").timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )  # noqa: E731
    pairs = []
    for rid, pid, cid, rating, body, created, updated in v1:
        val = {
            "before": None,
            "op": "u",
            "after": {
                "review_id": rid,
                "product_id": pid,
                "customer_id": cid,
                "rating": rating,
                "body": body,
                "created_at": fmt(created),
                "updated_at": fmt(updated),
            },
            "source": {"ts_ms": int(updated.timestamp() * 1000), "lsn": None},
        }
        pairs.append((key_of(rid), json.dumps(val, ensure_ascii=False).encode()))
    produce_raw(pairs)
    wait_drained()
    record(
        "S7_stale_replay",
        replayed=len(pairs),
        stale_skipped=stats("S7").get("stale_skipped", 0) - stale_before,
        **verify(e, since),
    )

    record("S9_final_check", **daily_check.run("final"))
    stop_consumer(cons)
    with open((LOG_DIR / "results.json"), "w", encoding="utf-8") as f:
        json.dump(RESULTS, f, ensure_ascii=False, indent=2, default=str)


if __name__ == "__main__":
    main()
