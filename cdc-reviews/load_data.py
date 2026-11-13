"""리뷰 원본과 가상 상품, 고객을 적재하고 분석 테이블을 준비한다."""

import argparse
import datetime as dt
import random

from common import PROJECT_ROOT, dw, pseudonym, src
from psycopg2.extras import execute_values

random.seed(42)
KW = [
    "피부",
    "촉촉",
    "발림",
    "향",
    "보습",
    "토너",
    "크림",
    "로션",
    "세럼",
    "에센스",
    "선크림",
    "트러블",
    "건성",
    "지성",
    "화장",
    "클렌징",
    "앰플",
    "수분",
]
BRANDS = ["브랜드A", "브랜드B", "브랜드C", "브랜드D", "브랜드E", "브랜드F", "브랜드G", "브랜드H"]
OTHER_CATS = ["생활", "식품", "패션", "가전"]
# 생성 시각을 과거로 두어 최신 수정이 이전 버전으로 판정되지 않게 한다.
NOW = dt.datetime.now(dt.timezone.utc)
TODAY = NOW.date()
SALE_DAYS = {TODAY - dt.timedelta(days=d) for d in (10, 11, 12)}


def past_time():
    return NOW - dt.timedelta(days=random.randint(1, 30), seconds=random.randint(0, 86399))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reviews", default=str(PROJECT_ROOT / "data" / "reviews.tsv"))
    args = parser.parse_args()
    rows = [l.rstrip("\n").split("\t", 1) for l in open(args.reviews, encoding="utf-8")]

    products = []
    for pid in range(1, 201):
        cat = "스킨케어" if pid <= 50 else random.choice(OTHER_CATS)
        products.append(
            (
                pid,
                random.choice(BRANDS),
                cat,
                random.choice(["1만원 미만", "1~3만원", "3만원 이상"]),
            )
        )
    skin_pids = [p[0] for p in products if p[2] == "스킨케어"]
    other_pids = [p[0] for p in products if p[2] != "스킨케어"]

    customers = [
        (
            cid,
            f"고객{cid}",
            f"010-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
            random.choice(["건성", "지성", "복합성", "민감성"]),
            random.choice(["20대", "30대", "40대", "50대 이상"]),
            random.choice(["일반", "실버", "골드", "VIP"]),
        )
        for cid in range(1, 5001)
    ]

    reviews = []
    for i, (rating, body) in enumerate(rows, start=1):
        is_skin = sum(k in body for k in KW) >= 2
        pid = random.choice(skin_pids if is_skin else other_pids)
        created = past_time()
        reviews.append((i, pid, random.randint(1, 5000), int(rating), body, created, created))

    # 개인정보 마스킹 시험용: 가짜 전화번호가 든 리뷰 100건
    for j in range(100):
        rid = len(rows) + 1 + j
        phone = f"010-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
        created = past_time()
        reviews.append(
            (
                rid,
                random.choice(skin_pids),
                random.randint(1, 5000),
                5,
                f"촉촉하고 좋아요 문의는 {phone} 로 주세요",
                created,
                created,
            )
        )

    with src() as c, c.cursor() as cur:
        execute_values(cur, "INSERT INTO products VALUES %s", products)
        execute_values(cur, "INSERT INTO customers VALUES %s", customers)
        execute_values(cur, "INSERT INTO reviews VALUES %s", reviews, page_size=5000)
    print(
        "source loaded:",
        len(products),
        "products,",
        len(customers),
        "customers,",
        len(reviews),
        "reviews",
    )

    with dw() as c, c.cursor() as cur:
        cur.execute(open(PROJECT_ROOT / "sql/dw_schema.sql", encoding="utf-8").read())
        execute_values(cur, "INSERT INTO d_product VALUES %s", products)
        execute_values(
            cur,
            "INSERT INTO d_customer VALUES %s",
            [(pseudonym(cid), st, ab, gr) for cid, _, _, st, ab, gr in customers],
        )
        days = [TODAY - dt.timedelta(days=d) for d in range(0, 40)]
        execute_values(
            cur,
            "INSERT INTO d_date VALUES %s",
            [
                (int(d.strftime("%Y%m%d")), d, "월화수목금토일"[d.weekday()], d in SALE_DAYS)
                for d in days
            ],
        )
    print("dw schema and dimensions ready")


if __name__ == "__main__":
    main()
