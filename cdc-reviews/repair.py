"""불일치한 리뷰를 원본 DB 기준으로 복구한다."""

import time

from common import content_hash, flags, mask, normalize, pseudonym
from psycopg2.extras import execute_values


def repair(dw_conn, src_conn, review_ids):
    ids = sorted(set(review_ids))
    if not ids:
        return 0
    with src_conn.cursor() as cur:
        cur.execute(
            "SELECT review_id, product_id, customer_id, rating, body, created_at, updated_at FROM reviews WHERE review_id = ANY(%s)",
            (ids,),
        )
        found = {r[0]: r for r in cur.fetchall()}
    src_conn.commit()

    base = -int(time.time() * 1_000_000)
    with dw_conn.cursor() as cur:
        rows, texts, events = [], [], []
        for i, rid in enumerate(ids):
            r = found.get(rid)
            if r is None:
                rows.append((rid, None, None, None, None, None, None, None, True))
                events.append((rid, "R", None, -1, base - i, None, None))
                continue
            _, pid, cid, rating, body, created, updated = r
            masked, pii = mask(body)
            norm = content_hash(normalize(body))
            cur.execute(
                "SELECT 1 FROM s_review_text WHERE norm_hash = %s AND review_id <> %s LIMIT 1",
                (norm, rid),
            )
            fk = flags(body, rating, cur.fetchone() is not None, pii)
            h = content_hash(body)
            rows.append(
                (
                    rid,
                    pid,
                    pseudonym(cid),
                    int(created.strftime("%Y%m%d")),
                    fk,
                    rating,
                    len(normalize(body)),
                    h,
                    False,
                    updated,
                )
            )
            texts.append((rid, masked, norm))
            events.append((rid, "R", None, -1, base - i, None, h))
        live = [r for r in rows if len(r) == 10]
        gone = [r[0] for r in rows if len(r) == 9]
        if live:
            execute_values(
                cur,
                """
                INSERT INTO f_review (review_id, product_key, customer_key, date_key, flag_key, rating, text_len, content_hash, is_deleted, src_version_ts)
                VALUES %s
                ON CONFLICT (review_id) DO UPDATE SET product_key=EXCLUDED.product_key, customer_key=EXCLUDED.customer_key,
                    date_key=EXCLUDED.date_key, flag_key=EXCLUDED.flag_key, rating=EXCLUDED.rating, text_len=EXCLUDED.text_len,
                    content_hash=EXCLUDED.content_hash, is_deleted=false, src_version_ts=EXCLUDED.src_version_ts, applied_at=clock_timestamp()""",
                live,
            )
            execute_values(
                cur,
                """INSERT INTO s_review_text VALUES %s
                ON CONFLICT (review_id) DO UPDATE SET masked_text=EXCLUDED.masked_text, norm_hash=EXCLUDED.norm_hash""",
                texts,
            )
        if gone:
            cur.execute(
                "UPDATE f_review SET is_deleted = true, applied_at = clock_timestamp() WHERE review_id = ANY(%s)",
                (gone,),
            )
            cur.execute("DELETE FROM s_review_text WHERE review_id = ANY(%s)", (gone,))
        # 복구도 이벤트로 남긴다 (op R, partition -1)
        execute_values(
            cur,
            """INSERT INTO f_review_event (review_id, op, source_ts, source_lsn, kafka_partition, kafka_offset, old_hash, new_hash)
            VALUES %s""",
            events,
            template="(%s, %s, now(), %s, %s, %s, %s, %s)",
        )
    dw_conn.commit()
    return len(ids)
