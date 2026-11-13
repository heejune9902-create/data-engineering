"""CDC와 배치 수집의 지연, 정합성과 자원 사용량을 집계한다."""

import json
import os
import statistics as st
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import dw, src  # noqa: E402

L = "batch_compare/logs"
rd = lambda f: float(open(f"{L}/{f}").read().strip())
t_start, t_ls, t_le = rd("t_start"), rd("t_load_start"), rd("t_load_end")
truth = [json.loads(l) for l in open(f"{L}/truth.jsonl", encoding="utf-8")]
rids = sorted({t["rid"] for t in truth})
ops = defaultdict(list)
for t in truth:
    ops[t["rid"]].append(t)


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


res = {}
# streaming
d = dw()
with d.cursor() as c:
    c.execute(
        """SELECT review_id, op, extract(epoch from source_ts), extract(epoch from ingested_at)
                 FROM f_review_event WHERE review_id = ANY(%s) AND ingested_at >= to_timestamp(%s)""",
        (rids, t_ls - 5),
    )
    ev = defaultdict(list)
    for rid, op, sts, ing in c.fetchall():
        ev[rid].append((op, float(sts), float(ing)))
    c.execute(
        "SELECT review_id, is_deleted, content_hash FROM f_review WHERE review_id = ANY(%s)",
        (rids,),
    )
    s_final = {r[0]: (r[1], r[2]) for r in c.fetchall()}
lat, missed, hist = [], 0, []
for t in truth:
    want = "d" if t["op"] == "d" else "u"
    m = [e for e in ev[t["rid"]] if e[0] == want and abs(e[1] - t["ts"]) < 2]
    if m:
        lat.append(min(e[2] for e in m) - t["ts"])
    else:
        missed += 1
for rid, ts in ops.items():
    if sum(1 for t in ts if t["op"] in ("u", "u2")) >= 2:
        hist.append(sum(1 for e in ev[rid] if e[0] == "u") >= 2)
s_del_missed = sum(
    1
    for t in truth
    if t["op"] == "d" and not any(e[0] == "d" and abs(e[1] - t["ts"]) < 2 for e in ev[t["rid"]])
)
res["stream"] = {
    "lat": lat,
    "missed": missed,
    "hist": hist,
    "final": s_final,
    "del_missed": s_del_missed,
}

# batch
for name in ["inc10", "inc60", "full60"]:
    runs = [json.loads(l) for l in open(f"{L}/{name}.jsonl", encoding="utf-8")]
    init = runs[0]
    runs = [r for r in runs if r["type"] == "run"]
    applied = defaultdict(list)  # rid -> [(src_ts, applied_ts)]
    dels = {}
    for r in runs:
        for rid, sts in r["applied"]:
            applied[rid].append((sts, r["applied_ts"]))
        for rid in r["deleted"]:
            dels.setdefault(rid, r["applied_ts"])
    lat, missed, superseded = [], 0, 0
    for t in truth:
        if t["op"] == "d":
            if t["rid"] in dels:
                lat.append(dels[t["rid"]] - t["ts"])
            else:
                missed += 1
            continue
        m = [a for a in applied[t["rid"]] if a[0] >= t["ts"] - 0.5]
        if m:
            first = min(m, key=lambda a: a[1])
            lat.append(first[1] - t["ts"])
            if first[0] > t["ts"] + 0.5:
                superseded += 1  # 이 수정은 따로 반영되지 못하고 다음 수정에 덮임
        else:
            missed += 1
    hist = []
    for rid, ts in ops.items():
        if sum(1 for t in ts if t["op"] in ("u", "u2")) >= 2:
            hist.append(len({a[0] for a in applied[rid]}) >= 2)
    with d.cursor() as c:
        c.execute(
            f"SELECT review_id, is_deleted, content_hash FROM bexp.{name} WHERE review_id = ANY(%s)",
            (rids,),
        )
        fin = {r[0]: (r[1], r[2]) for r in c.fetchall()}
    win = [r for r in runs if t_ls <= r["start"] <= t_le + 70]
    res[name] = {
        "lat": lat,
        "missed": missed,
        "superseded": superseded,
        "hist": hist,
        "final": fin,
        "init": init,
        "del_missed": sum(1 for t in truth if t["op"] == "d" and t["rid"] not in dels),
        "runs": len(win),
        "rows_read": sum(r["rows_read"] for r in win),
        "query_ms": sum(r["query_ms"] for r in win),
        "max_run_ms": max((r["total_ms"] for r in win), default=0),
        "avg_query_ms": st.mean([r["query_ms"] for r in win]) if win else 0,
    }

# 원본과 최종 상태 비교
s = src()
with s.cursor() as c:
    c.execute("SELECT review_id, body FROM reviews WHERE review_id = ANY(%s)", (rids,))
    from common import content_hash

    alive = {r[0]: content_hash(r[1]) for r in c.fetchall()}


def final_mismatch(fin):
    bad_del, bad_hash = 0, 0
    for rid in rids:
        if rid not in alive:
            if rid in fin and not fin[rid][0]:
                bad_del += 1
        elif rid in fin and fin[rid][1] != alive[rid]:
            bad_hash += 1
    return bad_del, bad_hash


# 자원
proc, dock = [], []
for l in open(f"{L}/resources.jsonl", encoding="utf-8"):
    x = json.loads(l)
    (proc if x["type"] == "proc" else dock).append(x)


def proc_stats(lab, a, b):
    pts = [(x["t"], x["procs"][lab]) for x in proc if lab in x["procs"] and a <= x["t"] <= b]
    if len(pts) < 2:
        return None
    cpu = pts[-1][1]["cpu_s"] - pts[0][1]["cpu_s"]
    rss = [p[1]["rss_mb"] for p in pts]
    return {
        "cpu_s": cpu,
        "cpu_pct": cpu / (pts[-1][0] - pts[0][0]) * 100,
        "rss_avg": st.mean(rss),
        "rss_max": max(rss),
    }


def cont_stats(name, a, b):
    pts = [x["cont"][name] for x in dock if name in x["cont"] and a <= x["t"] <= b]
    if not pts:
        return None
    return {
        "cpu_avg": st.mean(p["cpu_pct"] for p in pts),
        "cpu_max": max(p["cpu_pct"] for p in pts),
        "mem_avg": st.mean(p["mem_mb"] for p in pts),
    }


W = (t_ls, t_le + 60)
IDLE = (t_start + 25, t_ls - 2)
labs = {"stream": "stream_consumer", "inc10": "inc10", "inc60": "inc60", "full60": "full60"}
P = {k: proc_stats(v, *W) for k, v in labs.items()}
PI = {k: proc_stats(v, *IDLE) for k, v in labs.items()}
C = {n: cont_stats(n, *W) for n in ["demo-postgres", "demo-kafka", "demo-connect"]}
CI = {n: cont_stats(n, *IDLE) for n in ["demo-postgres", "demo-kafka", "demo-connect"]}

f = lambda v, n=1: "-" if v is None else f"{v:,.{n}f}"
names = {
    "stream": "스트리밍 (CDC)",
    "inc10": "증분 배치 10초",
    "inc60": "증분 배치 60초",
    "full60": "전체 비교 배치 60초",
}
n_u = sum(1 for t in truth if t["op"] != "d")
n_d = sum(1 for t in truth if t["op"] == "d")
lines = [
    f"# 배치 vs 스트리밍 비교 (같은 변경 부하)",
    "",
    f"부하: {t_le - t_ls:.0f}초 동안 변경 {len(truth)}건 (수정 {n_u}, 삭제 {n_d}), 초당 약 {len(truth) / (t_le - t_ls):.1f}건. "
    "네 방식을 동시에 돌리고 같은 정답 로그로 채점. 시각은 모두 DB 시계 기준.",
    "",
    "## 정확도와 지연",
    "",
    "| 방식 | 반영 지연 p50 | p95 | 최대 | 반영 못 한 변경 | 그중 삭제 | 중간 수정 이력 보존 | 최종 상태 불일치 (삭제 미반영 / 본문 다름) |",
    "|---|---|---|---|---|---|---|---|",
]
for k in ["stream", "inc10", "inc60", "full60"]:
    r = res[k]
    h = r["hist"]
    bd, bh = final_mismatch(r["final"])
    dm = r["del_missed"]
    lines.append(
        f"| {names[k]} | {f(pct(r['lat'], 0.5), 2)}초 | {f(pct(r['lat'], 0.95), 2)}초 | {f(max(r['lat']) if r['lat'] else None, 2)}초 | "
        f"{r['missed']} / {len(truth)} | {'-' if dm is None else dm} | {sum(h)} / {len(h)} | {bd} / {bh} |"
    )
lines += [
    "",
    "## 원본 DB 조회 부하 (부하 구간)",
    "",
    "| 방식 | 조회 횟수 | 원본에서 읽은 행 | 조회 시간 합계 | 1회 평균 조회 | 1회 최장 처리 |",
    "|---|---|---|---|---|---|",
    f"| 스트리밍 (CDC) | 0 (변경 로그만 읽음) | 변경 {len(truth)}건분 | - | - | - |",
]
for k in ["inc10", "inc60", "full60"]:
    r = res[k]
    lines.append(
        f"| {names[k]} | {r['runs']} | {r['rows_read']:,} | {f(r['query_ms'] / 1000, 2)}초 | {f(r['avg_query_ms'], 0)}ms | {f(r['max_run_ms'], 0)}ms |"
    )
lines += [
    "",
    "## 컴퓨팅 자원 (부하 구간 평균, 괄호는 부하 전 대기 상태)",
    "",
    "| 수집 프로세스 | CPU 사용 시간 | 평균 CPU | 평균 메모리 | 최대 메모리 |",
    "|---|---|---|---|---|",
]
for k in ["stream", "inc10", "inc60", "full60"]:
    p, pi = P[k], PI[k]
    if not p:
        lines.append(f"| {names[k]} | - | - | - | - |")
        continue
    lines.append(
        f"| {names[k]} {'컨슈머' if k == 'stream' else '로더'} | {f(p['cpu_s'], 1)}초 | {f(p['cpu_pct'], 1)}% ({f(pi['cpu_pct'] if pi else None, 1)}%) | "
        f"{f(p['rss_avg'], 0)}MB | {f(p['rss_max'], 0)}MB |"
    )
lines += ["", "| 상시 인프라 컨테이너 | 평균 CPU | 최대 CPU | 평균 메모리 |", "|---|---|---|---|"]
for n in ["demo-kafka", "demo-connect", "demo-postgres"]:
    c, ci = C[n], CI[n]
    lines.append(
        f"| {n} | {f(c['cpu_avg'], 1)}% ({f(ci['cpu_avg'] if ci else None, 1)}%) | {f(c['cpu_max'], 1)}% | {f(c['mem_avg'], 0)}MB ({f(ci['mem_avg'] if ci else None, 0)}MB) |"
    )
lines += [
    "",
    f"초기 적재(비교 제외): "
    + ", ".join(
        f"{names[k]} {res[k]['init']['rows']:,}행 {res[k]['init']['sec']}초"
        for k in ["inc10", "inc60", "full60"]
    ),
]
open("batch_compare/result.md", "w", encoding="utf-8").write("\n".join(lines) + "\n")
print("\n".join(lines))
