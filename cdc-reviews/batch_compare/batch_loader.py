"""증분 조회와 전체 조회 방식으로 리뷰를 주기적으로 반영한다."""

import argparse
import json
import os
import re
import sys
import time

from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import content_hash, dw, src  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--mode", choices=["inc", "full"], required=True)
ap.add_argument("--interval", type=float, required=True)
ap.add_argument("--table", required=True)
ap.add_argument("--duration", type=int, required=True)
args = ap.parse_args()
if not re.fullmatch(r"[a-z][a-z0-9_]*", args.table):
    ap.error("--table must be a lowercase SQL identifier")
T = f"bexp.{args.table}"
log = open(f"batch_compare/logs/{args.table}.jsonl", "w", encoding="utf-8")

s, d = src(), dw()
with d.cursor() as c:
    c.execute(f"DROP TABLE IF EXISTS {T}")
    c.execute(f"""CREATE TABLE {T} (review_id bigint PRIMARY KEY, rating smallint, content_hash text,
                  is_deleted boolean NOT NULL DEFAULT false, src_updated_at timestamptz, applied_at timestamptz)""")
d.commit()

# 시작 시점 상태를 한 번 통째로 맞춘다 (초기 적재, 비교 대상에서 제외)
t0 = time.time()
with s.cursor() as c:
    c.execute("SELECT review_id, rating, body, updated_at FROM reviews")
    rows = c.fetchall()
with d.cursor() as c:
    execute_values(
        c,
        f"INSERT INTO {T} VALUES %s",
        [(r[0], r[1], content_hash(r[2]), False, r[3], None) for r in rows],
        page_size=5000,
    )
d.commit()
watermark = max(r[3] for r in rows)
log.write(json.dumps({"type": "init", "rows": len(rows), "sec": round(time.time() - t0, 3)}) + "\n")
del rows

t_end = time.time() + args.duration
next_run = time.time() + args.interval
while time.time() < t_end:
    time.sleep(max(0.0, next_run - time.time()))
    next_run += args.interval
    run_start = time.time()
    with s.cursor() as c:
        q0 = time.time()
        if args.mode == "inc":
            c.execute(
                "SELECT review_id, rating, body, updated_at FROM reviews WHERE updated_at > %s ORDER BY updated_at",
                (watermark,),
            )
        else:
            c.execute("SELECT review_id, rating, body, updated_at FROM reviews")
        rows = c.fetchall()
        query_ms = (time.time() - q0) * 1000
    s.commit()
    applied, deleted_ids = [], []
    with d.cursor() as c:
        if args.mode == "inc":
            todo = rows
            if rows:
                watermark = max(watermark, max(r[3] for r in rows))
        else:
            c.execute(f"SELECT review_id, content_hash, is_deleted FROM {T}")
            cur_state = {r[0]: (r[1], r[2]) for r in c.fetchall()}
            todo = [r for r in rows if cur_state.get(r[0], (None, True))[0] != content_hash(r[2])]
            src_ids = {r[0] for r in rows}
            deleted_ids = [
                rid for rid, (h, dele) in cur_state.items() if not dele and rid not in src_ids
            ]
        if todo:
            execute_values(
                c,
                f"""INSERT INTO {T} (review_id, rating, content_hash, is_deleted, src_updated_at, applied_at)
                VALUES %s ON CONFLICT (review_id) DO UPDATE SET rating=EXCLUDED.rating, content_hash=EXCLUDED.content_hash,
                src_updated_at=EXCLUDED.src_updated_at, applied_at=EXCLUDED.applied_at""",
                [(r[0], r[1], content_hash(r[2]), False, r[3]) for r in todo],
                template="(%s,%s,%s,%s,%s,clock_timestamp())",
                page_size=2000,
            )
            applied = [(r[0], r[3].timestamp()) for r in todo]
        if deleted_ids:
            c.execute(
                f"UPDATE {T} SET is_deleted=true, applied_at=clock_timestamp() WHERE review_id = ANY(%s)",
                (deleted_ids,),
            )
        c.execute("SELECT extract(epoch from clock_timestamp())")
        applied_db_ts = float(c.fetchone()[0])
    d.commit()
    log.write(
        json.dumps(
            {
                "type": "run",
                "start": run_start,
                "applied_ts": applied_db_ts,
                "rows_read": len(rows),
                "rows_applied": len(applied),
                "deleted_marked": len(deleted_ids),
                "query_ms": round(query_ms, 1),
                "total_ms": round((time.time() - run_start) * 1000, 1),
                "applied": applied,
                "deleted": deleted_ids,
            }
        )
        + "\n"
    )
    log.flush()
    del rows
print(f"{args.table} done", flush=True)
