"""수정, 재수정과 삭제 부하를 생성한다."""

import argparse
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import content_hash, src  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--duration", type=int, default=300)
ap.add_argument("--rate", type=float, default=5.0, help="초당 변경 건수")
ap.add_argument("--out", default="batch_compare/logs/truth.jsonl")
args = ap.parse_args()

rng = random.Random(7)
conn = src()
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT review_id FROM reviews WHERE review_id BETWEEN 1000 AND 199000")
pool = [r[0] for r in cur.fetchall()]
rng.shuffle(pool)
recent = []  # (rid, t) 최근 수정한 리뷰, 재수정 후보
deleted = set()
out = open(args.out, "w", encoding="utf-8")
t_end = time.time() + args.duration
n = 0
while time.time() < t_end:
    tick = time.time()
    r = rng.random()
    if r < 0.10 and recent:
        rid = rng.choice(recent[-50:])[0]
        op = "u2"
    elif r < 0.30:
        rid = pool.pop()
        op = "d"
    else:
        rid = pool.pop()
        op = "u"
    if rid in deleted:
        continue
    if op == "d":
        cur.execute("DELETE FROM reviews WHERE review_id=%s RETURNING clock_timestamp()", (rid,))
        row = cur.fetchone()
        if not row:
            continue
        deleted.add(rid)
        rec = {"rid": rid, "op": "d", "ts": row[0].timestamp()}
    else:
        tag = f" [수정{n}]"
        cur.execute(
            """UPDATE reviews SET body = body || %s, rating = %s, updated_at = clock_timestamp()
                       WHERE review_id=%s RETURNING clock_timestamp(), body""",
            (tag, rng.randint(1, 5), rid),
        )
        row = cur.fetchone()
        if not row:
            continue
        recent.append((rid, tick))
        rec = {"rid": rid, "op": op, "ts": row[0].timestamp(), "hash": content_hash(row[1])}
    out.write(json.dumps(rec) + "\n")
    out.flush()
    n += 1
    time.sleep(max(0.0, 1.0 / args.rate - (time.time() - tick)))
print(f"workload done: {n} changes", flush=True)
