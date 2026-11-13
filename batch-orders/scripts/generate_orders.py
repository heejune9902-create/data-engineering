"""브랜드별 가상 고객과 주문을 생성한다."""

import argparse
import os
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

BRANDS = {
    # brand: (고객 비율, 평균 결제액, 카테고리)
    "food": (0.8, 18000, ["간편식", "음료", "베이커리", "신선"]),
    "beauty": (0.5, 32000, ["스킨케어", "메이크업", "헤어", "향수"]),
    "cinema": (0.4, 14000, ["일반관", "특별관", "매점"]),
}


def phone_format(nums, brand):
    """브랜드마다 전화번호를 다르게 저장한다."""
    s = pd.Series(nums).astype(str)
    if brand == "food":
        return "010-" + s.str[:4] + "-" + s.str[4:]
    if brand == "beauty":
        return "010" + s
    return "+82 10 " + s.str[:4] + " " + s.str[4:]


def make_customer_id(idx, brand):
    if brand == "food":
        return "F" + pd.Series(idx).astype(str).str.zfill(8)
    if brand == "beauty":
        return pd.Series(idx + 50_000_000).astype(str)
    return "C-" + pd.Series(idx).map(lambda x: format(x * 7919 % 16**8, "08x"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--people", type=int, default=200_000)
    ap.add_argument("--orders", type=int, default=3_000_000)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--start", default="2026-08-01")
    ap.add_argument(
        "--out",
        default=str(
            Path(os.environ.get("ORDERS_DATA_ROOT", Path(__file__).resolve().parents[1] / "data"))
            / "raw"
        ),
    )
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    rng = np.random.default_rng(a.seed)
    start = date.fromisoformat(a.start)

    # 가상 고객: 전화번호 8자리와 통합 멤버십 번호(일부만 가입)
    phones = rng.choice(10**8, size=a.people, replace=False)
    phones = pd.Series(phones).astype(str).str.zfill(8).to_numpy()
    member = np.where(
        rng.random(a.people) < 0.6,
        pd.Series(rng.choice(10**9, a.people, replace=False)).astype(str).str.zfill(12),
        None,
    )

    weights = np.array([v[0] for v in BRANDS.values()])
    brand_orders = (weights / weights.sum() * a.orders).astype(int)

    for (brand, (ratio, mean_amt, cats)), n_orders in zip(BRANDS.items(), brand_orders):
        # 이 브랜드를 쓰는 사람만 뽑는다
        who = np.sort(rng.choice(a.people, size=int(a.people * ratio), replace=False))
        customers = pd.DataFrame(
            {
                "brand_customer_id": make_customer_id(np.arange(len(who)), brand),
                "phone": phone_format(phones[who], brand),
                # 멤버십 번호를 브랜드에 등록하지 않은 사람도 있다
                "membership_no": np.where(rng.random(len(who)) < 0.7, member[who], None),
                "joined_at": [
                    start - timedelta(days=int(d)) for d in rng.integers(0, 900, len(who))
                ],
            }
        )
        cdir = os.path.join(a.out, "customers", f"brand={brand}")
        os.makedirs(cdir, exist_ok=True)
        customers.to_csv(os.path.join(cdir, "customers.csv"), index=False)

        # 주문: 소수 고객이 많이 사고, 매장 1곳(S001)에 주문이 몰린다
        buyer = (rng.pareto(1.5, n_orders) * 50).astype(int) % len(customers)
        n_store = 300
        store = rng.integers(2, n_store + 1, n_orders)
        store[rng.random(n_orders) < 0.25] = 1
        day = rng.integers(0, a.days, n_orders)
        orders = pd.DataFrame(
            {
                "order_id": [f"{brand[:2].upper()}{i:010d}" for i in range(n_orders)],
                "brand_customer_id": customers["brand_customer_id"].to_numpy()[buyer],
                "store_id": pd.Series(store).map(lambda s: f"S{s:03d}"),
                "order_ts": [start + timedelta(days=int(d)) for d in day],
                "category": rng.choice(cats, n_orders),
                "amount": np.round(rng.gamma(2.0, mean_amt / 2.0, n_orders), -1).astype(int),
            }
        )
        orders["order_ts"] = pd.to_datetime(orders["order_ts"]) + pd.to_timedelta(
            rng.integers(0, 86400, n_orders), unit="s"
        )
        orders["dt"] = orders["order_ts"].dt.strftime("%Y-%m-%d")
        for dt, part in orders.groupby("dt"):
            odir = os.path.join(a.out, "orders", f"brand={brand}", f"dt={dt}")
            os.makedirs(odir, exist_ok=True)
            part.drop(columns="dt").to_csv(os.path.join(odir, "orders.csv"), index=False)
        print(f"{brand}: customers {len(customers):,}, orders {n_orders:,}")


if __name__ == "__main__":
    main()
