"""리뷰 변경을 분석 DB에 반영하고 DB 커밋 후 오프셋을 커밋한다."""

import argparse
import datetime as dt
import json
import os
import random
import time

from common import (
    BOOTSTRAP,
    CUSTOMER_TOPIC,
    DLT_TOPIC,
    GROUP,
    LOG_DIR,
    PRODUCT_TOPIC,
    TOPIC,
    content_hash,
    dw,
    flags,
    mask,
    normalize,
    pseudonym,
)
from confluent_kafka import Consumer, Producer
from psycopg2.extras import execute_values

MAX_RETRY = 3


def apply_dims(conn, msgs):
    """상품과 고객 변경을 디멘션에 덮어쓴다(SCD 1). 이름과 연락처는 d_customer로 넘기지 않는다.

    원본에서 삭제돼도 디멘션 행은 남긴다. 과거 리뷰가 그 상품과 고객을 계속 참조하기 때문이다.
    """
    products, customers = {}, {}
    for m in msgs:
        v = json.loads(m.value())
        if v["op"] == "d":
            continue
        r = v["after"]
        if m.topic() == PRODUCT_TOPIC:
            products[r["product_id"]] = (
                r["product_id"],
                r["brand"],
                r["category"],
                r["price_band"],
            )
        else:
            key = pseudonym(r["customer_id"])
            customers[key] = (key, r["skin_type"], r["age_band"], r["grade"])
    with conn.cursor() as cur:
        if products:
            execute_values(
                cur,
                """INSERT INTO d_product VALUES %s ON CONFLICT (product_key) DO UPDATE SET
                brand = EXCLUDED.brand, category = EXCLUDED.category, price_band = EXCLUDED.price_band""",
                list(products.values()),
            )
        if customers:
            execute_values(
                cur,
                """INSERT INTO d_customer VALUES %s ON CONFLICT (customer_key) DO UPDATE SET
                skin_type = EXCLUDED.skin_type, age_band = EXCLUDED.age_band, grade = EXCLUDED.grade""",
                list(customers.values()),
            )
    conn.commit()
    return len(products) + len(customers)


def ts_from_iso(s):
    return dt.datetime.fromisoformat(s)


def ts_from_ms(ms):
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc)


def load_fail_ids(path):
    if not path or not os.path.exists(path):
        return set()
    return {int(x) for x in open(path).read().split() if x.strip()}


def parse(msg, fail_ids):
    """Debezium 이벤트 하나를 반영할 형태로 바꾼다. 실패하면 예외를 던진다."""
    v = json.loads(msg.value())
    op = v["op"]
    before, after, source = v.get("before"), v.get("after"), v["source"]
    row = after if op != "d" else before
    rid = int(row["review_id"])
    if rid in fail_ids and op != "d":
        raise ValueError(f"injected processing failure for review {rid}")
    if op == "d":
        version = ts_from_ms(source["ts_ms"])
    else:
        version = ts_from_iso(after["updated_at"])
    return {
        "review_id": rid,
        "op": op,
        "source_ts": ts_from_ms(source["ts_ms"]),
        "lsn": source.get("lsn"),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "old_hash": content_hash(before["body"]) if before else None,
        "new_hash": content_hash(after["body"]) if after else None,
        "version": version,
        "row": row,
    }


def apply_batch(conn, events, stats, run_id):
    with conn.cursor() as cur:
        inserted = execute_values(
            cur,
            """
            INSERT INTO f_review_event (review_id, op, source_ts, source_lsn, kafka_partition, kafka_offset, old_hash, new_hash)
            VALUES %s ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING RETURNING 1""",
            [
                (
                    e["review_id"],
                    e["op"],
                    e["source_ts"],
                    e["lsn"],
                    e["partition"],
                    e["offset"],
                    e["old_hash"],
                    e["new_hash"],
                )
                for e in events
            ],
            fetch=True,
        )
        dup = len(events) - len(inserted)

        # 같은 배치 안의 같은 리뷰는 가장 새 버전 하나로 모은다
        latest = {}
        for e in events:
            cur_e = latest.get(e["review_id"])
            if cur_e is None or (e["version"], e["offset"]) >= (cur_e["version"], cur_e["offset"]):
                latest[e["review_id"]] = e
        ups = list(latest.values())

        live = [e for e in ups if e["op"] != "d"]
        norm = {e["review_id"]: content_hash(normalize(e["row"]["body"])) for e in live}
        dup_norm = set()
        if live:
            cur.execute(
                "SELECT norm_hash FROM s_review_text WHERE norm_hash = ANY(%s) AND NOT (review_id = ANY(%s))",
                (list(norm.values()), list(norm.keys())),
            )
            dup_norm = {r[0] for r in cur.fetchall()}

        rows, texts = [], []
        for e in ups:
            r = e["row"]
            if e["op"] == "d":
                rows.append(
                    (e["review_id"], None, None, None, None, None, None, None, True, e["version"])
                )
                continue
            masked, pii = mask(r["body"])
            fk = flags(r["body"], r["rating"], norm[e["review_id"]] in dup_norm, pii)
            created = ts_from_iso(r["created_at"])
            rows.append(
                (
                    e["review_id"],
                    r["product_id"],
                    pseudonym(r["customer_id"]),
                    int(created.strftime("%Y%m%d")),
                    fk,
                    r["rating"],
                    len(normalize(r["body"])),
                    e["new_hash"],
                    False,
                    e["version"],
                )
            )
            texts.append((e["review_id"], masked, norm[e["review_id"]]))

        applied = execute_values(
            cur,
            """
            INSERT INTO f_review (review_id, product_key, customer_key, date_key, flag_key, rating, text_len, content_hash, is_deleted, src_version_ts)
            VALUES %s
            ON CONFLICT (review_id) DO UPDATE SET
                product_key = COALESCE(EXCLUDED.product_key, f_review.product_key),
                customer_key = COALESCE(EXCLUDED.customer_key, f_review.customer_key),
                date_key = COALESCE(EXCLUDED.date_key, f_review.date_key),
                flag_key = COALESCE(EXCLUDED.flag_key, f_review.flag_key),
                rating = COALESCE(EXCLUDED.rating, f_review.rating),
                text_len = COALESCE(EXCLUDED.text_len, f_review.text_len),
                content_hash = COALESCE(EXCLUDED.content_hash, f_review.content_hash),
                is_deleted = EXCLUDED.is_deleted,
                src_version_ts = EXCLUDED.src_version_ts,
                applied_at = clock_timestamp()
            WHERE EXCLUDED.src_version_ts >= f_review.src_version_ts
            RETURNING review_id""",
            rows,
            fetch=True,
        )
        applied_ids = {r[0] for r in applied}
        stale = len(rows) - len(applied_ids)

        texts = [t for t in texts if t[0] in applied_ids]
        if texts:
            execute_values(
                cur,
                """
                INSERT INTO s_review_text VALUES %s
                ON CONFLICT (review_id) DO UPDATE SET masked_text = EXCLUDED.masked_text, norm_hash = EXCLUDED.norm_hash""",
                texts,
            )
        deleted_ids = [
            e["review_id"] for e in ups if e["op"] == "d" and e["review_id"] in applied_ids
        ]
        if deleted_ids:
            cur.execute("DELETE FROM s_review_text WHERE review_id = ANY(%s)", (deleted_ids,))

        stats["applied"] += len(applied_ids)
        stats["dup"] += dup
        stats["stale"] += stale
        cur.execute(
            """UPDATE consumer_stats SET processed=%s, applied=%s, dup_events=%s, stale_skipped=%s, dropped=%s, sent_to_dlt=%s
                       WHERE id=%s""",
            (
                stats["processed"],
                stats["applied"],
                stats["dup"],
                stats["stale"],
                stats["dropped"],
                stats["dlt"],
                run_id,
            ),
        )
    conn.commit()


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="run")
    ap.add_argument("--drop-rate", type=float, default=0.0)
    ap.add_argument("--commit-every", type=int, default=1)
    ap.add_argument("--fail-ids", default=(LOG_DIR / "fail_ids.txt"))
    ap.add_argument(
        "--idle-exit",
        type=float,
        default=0,
        help="이 시간(초) 동안 메시지가 없으면 종료. 0이면 계속 돈다",
    )
    args = ap.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 600000,
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    consumer.subscribe([TOPIC, PRODUCT_TOPIC, CUSTOMER_TOPIC])
    conn = dw()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO consumer_stats (run_label, processed, applied, dup_events, stale_skipped, dropped, sent_to_dlt) "
            "VALUES (%s,0,0,0,0,0,0) RETURNING id",
            (args.label,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()

    stats = {"processed": 0, "applied": 0, "dup": 0, "stale": 0, "dropped": 0, "dlt": 0}
    rng = random.Random()
    batches = 0
    last_msg = time.time()
    print(f"consumer {args.label} started (run_id={run_id})", flush=True)
    try:
        while True:
            if os.path.exists((LOG_DIR / "STOP")):
                break
            msgs = consumer.consume(num_messages=500, timeout=0.5)
            msgs = [m for m in msgs if m.error() is None]
            if not msgs:
                if args.idle_exit and time.time() - last_msg > args.idle_exit:
                    break
                continue
            last_msg = time.time()
            dim_msgs = [m for m in msgs if m.topic() != TOPIC]
            if dim_msgs:
                apply_dims(conn, dim_msgs)
            msgs = [m for m in msgs if m.topic() == TOPIC]
            fail_ids = load_fail_ids(args.fail_ids)
            events = []
            for m in msgs:
                stats["processed"] += 1
                if args.drop_rate and rng.random() < args.drop_rate:
                    stats["dropped"] += 1
                    continue
                err = None
                for _ in range(MAX_RETRY):
                    try:
                        events.append(parse(m, fail_ids))
                        err = None
                        break
                    except Exception as ex:  # noqa: BLE001
                        err = ex
                        time.sleep(0.05)
                if err is not None:
                    send_to_dlt(producer, conn, m, err)
                    stats["dlt"] += 1
            if events:
                apply_batch(conn, events, stats, run_id)
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE consumer_stats SET processed=%s, dropped=%s, sent_to_dlt=%s WHERE id=%s",
                        (stats["processed"], stats["dropped"], stats["dlt"], run_id),
                    )
                conn.commit()
            batches += 1
            if batches % args.commit_every == 0:
                consumer.commit(asynchronous=False)
    finally:
        try:
            consumer.commit(asynchronous=False)
        except Exception:  # noqa: BLE001  커밋할 새 오프셋이 없을 때
            pass
        consumer.close()
        producer.flush(5)
        print(f"consumer {args.label} stopped: {stats}", flush=True)


def send_to_dlt(producer, conn, msg, err):
    try:
        rid = json.loads(msg.key())["review_id"]
    except Exception:  # noqa: BLE001
        rid = None
    payload = {
        "error": str(err),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "review_id": rid,
        "value": (msg.value() or b"").decode("utf-8", "replace"),
    }
    producer.produce(DLT_TOPIC, key=msg.key(), value=json.dumps(payload, ensure_ascii=False))
    producer.flush(5)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dlt_log (review_id, kafka_partition, kafka_offset, error) VALUES (%s,%s,%s,%s)",
            (rid, msg.partition(), msg.offset(), str(err)[:500]),
        )
    conn.commit()


if __name__ == "__main__":
    main()
