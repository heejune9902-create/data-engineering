"""DLT에 기록된 리뷰를 원본 DB 기준으로 복구한다."""

import json

from common import BOOTSTRAP, DLT_TOPIC, dw, src
from confluent_kafka import Consumer
from repair import repair


def run():
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "dlt-recovery",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([DLT_TOPIC])
    ids, idle = [], 0
    while idle < 6:
        msgs = c.consume(num_messages=500, timeout=1.0)
        msgs = [m for m in msgs if m.error() is None]
        if not msgs:
            idle += 1
            continue
        idle = 0
        for m in msgs:
            rid = json.loads(m.value()).get("review_id")
            if rid is not None:
                ids.append(int(rid))
    d, s = dw(), src()
    n = repair(d, s, ids)
    with d.cursor() as cur:
        cur.execute(
            "UPDATE dlt_log SET recovered_at = clock_timestamp() WHERE recovered_at IS NULL AND review_id = ANY(%s)",
            (ids,),
        )
    d.commit()
    if ids:
        c.commit(asynchronous=False)
    c.close()
    return {"dlt_messages": len(ids), "reviews_repaired": n}


if __name__ == "__main__":
    print(run())
