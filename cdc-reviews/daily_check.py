"""원본과 분석 DB의 리뷰 ID 및 본문 해시를 대조한다."""

import argparse
import json

from common import content_hash, dw, src
from repair import repair


def compare(src_conn, dw_conn):
    with src_conn.cursor() as cur:
        cur.execute("SELECT review_id, body FROM reviews")
        source = {rid: content_hash(body) for rid, body in cur.fetchall()}
    src_conn.commit()
    with dw_conn.cursor() as cur:
        cur.execute("SELECT review_id, content_hash FROM f_review WHERE NOT is_deleted")
        live = dict(cur.fetchall())
    dw_conn.commit()
    missing = [r for r in source if r not in live]
    mismatch = [r for r in source if r in live and live[r] != source[r]]
    delete_unapplied = [r for r in live if r not in source]
    return source, live, missing, mismatch, delete_unapplied


def run(label, do_repair=True):
    s, d = src(), dw()
    source, live, missing, mismatch, gone = compare(s, d)
    recovered = 0
    unrecovered = len(missing) + len(mismatch) + len(gone)
    if do_repair and unrecovered:
        recovered = repair(d, s, missing + mismatch + gone)
        _, _, m2, mm2, g2 = compare(s, d)
        unrecovered = len(m2) + len(mm2) + len(g2)
    with d.cursor() as cur:
        cur.execute(
            """INSERT INTO dq_check (run_label, source_live, dw_live, missing, hash_mismatch, delete_unapplied, recovered, unrecovered)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                label,
                len(source),
                len(live),
                len(missing),
                len(mismatch),
                len(gone),
                recovered,
                unrecovered,
            ),
        )
    d.commit()
    result = {
        "source_live": len(source),
        "dw_live": len(live),
        "missing": len(missing),
        "hash_mismatch": len(mismatch),
        "delete_unapplied": len(gone),
        "recovered": recovered,
        "unrecovered": unrecovered,
    }
    s.close()
    d.close()
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="manual")
    ap.add_argument("--no-repair", action="store_true")
    a = ap.parse_args()
    result = run(a.label, not a.no_repair)
    level = "ERROR" if result["unrecovered"] else "INFO"
    print(f"======= [{level}] 정합성 검사: {a.label} =======")
    print(json.dumps(result, ensure_ascii=False))
