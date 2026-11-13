"""리뷰를 생성, 수정, 삭제하고 검증용 변경 로그를 남긴다."""

import json
import time

from common import LOG_DIR, content_hash, src

TRUTH = LOG_DIR / "truth.jsonl"


def _log(scenario, entries):
    with open(TRUTH, "a", encoding="utf-8") as f:
        for e in entries:
            e["scenario"] = scenario
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def pick(n, exclude=()):
    with src() as c, c.cursor() as cur:
        cur.execute(
            "SELECT review_id FROM reviews WHERE review_id <= 200000 AND NOT (review_id = ANY(%s)) ORDER BY random() LIMIT %s",
            (list(exclude), n),
        )
        return [r[0] for r in cur.fetchall()]


def update(scenario, ids, tag, chunk=100, pause=0.0):
    out = []
    with src() as c:
        for i in range(0, len(ids), chunk):
            with c.cursor() as cur:
                cur.execute(
                    """UPDATE reviews SET body = body || %s, updated_at = clock_timestamp()
                               WHERE review_id = ANY(%s) RETURNING review_id, body, updated_at""",
                    (f" ({tag})", ids[i : i + chunk]),
                )
                rows = cur.fetchall()
            c.commit()
            entries = [
                {"op": "u", "review_id": r[0], "hash": content_hash(r[1]), "ts": r[2].isoformat()}
                for r in rows
            ]
            _log(scenario, entries)
            out += entries
            if pause:
                time.sleep(pause)
    return out


def delete(scenario, ids, chunk=100, pause=0.0):
    out = []
    with src() as c:
        for i in range(0, len(ids), chunk):
            with c.cursor() as cur:
                cur.execute(
                    "DELETE FROM reviews WHERE review_id = ANY(%s) RETURNING review_id",
                    (ids[i : i + chunk],),
                )
                rows = cur.fetchall()
            c.commit()
            entries = [
                {"op": "d", "review_id": r[0], "hash": None, "ts": time.time()} for r in rows
            ]
            _log(scenario, entries)
            out += entries
            if pause:
                time.sleep(pause)
    return out


def insert(scenario, n, chunk=100, pause=0.0):
    out = []
    with src() as c:
        for i in range(0, n, chunk):
            k = min(chunk, n - i)
            with c.cursor() as cur:
                cur.execute(
                    """INSERT INTO reviews (review_id, product_id, customer_id, rating, body, created_at, updated_at)
                               SELECT nextval('review_id_seq'), 1 + (random()*49)::int, 1 + (random()*4999)::int, 5,
                                      '새로 쓴 리뷰 ' || g || ' 촉촉하고 좋아요', clock_timestamp(), clock_timestamp()
                               FROM generate_series(1, %s) g RETURNING review_id, body, updated_at""",
                    (k,),
                )
                rows = cur.fetchall()
            c.commit()
            entries = [
                {"op": "c", "review_id": r[0], "hash": content_hash(r[1]), "ts": r[2].isoformat()}
                for r in rows
            ]
            _log(scenario, entries)
            out += entries
            if pause:
                time.sleep(pause)
    return out


def fetch_rows(ids):
    with src() as c, c.cursor() as cur:
        cur.execute(
            "SELECT review_id, product_id, customer_id, rating, body, created_at, updated_at FROM reviews WHERE review_id = ANY(%s)",
            (ids,),
        )
        return cur.fetchall()
