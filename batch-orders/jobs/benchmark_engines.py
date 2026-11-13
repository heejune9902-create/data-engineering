"""pandas와 Spark의 CSV 읽기 및 매장별 집계 시간을 비교한다."""

import argparse
import os
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    project_root = Path(__file__).resolve().parents[1]
    data_root = Path(os.environ.get("ORDERS_DATA_ROOT", project_root / "data"))
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=Path, default=data_root / "raw" / "orders")
    args = parser.parse_args()
    files = sorted(args.orders.rglob("*.csv"))
    if not files:
        raise FileNotFoundError(f"주문 CSV가 없습니다: {args.orders}")

    started = time.perf_counter()
    orders = pd.concat((pd.read_csv(path) for path in files), ignore_index=True)
    read_seconds = time.perf_counter() - started
    started = time.perf_counter()
    store_counts = orders.groupby("store_id").size().sort_values(ascending=False).head(5)
    aggregate_seconds = time.perf_counter() - started
    print(store_counts)
    print(f"[INFO] pandas 읽기 {read_seconds:.2f}s, 집계 {aggregate_seconds:.2f}s")
    print(f"[INFO] pandas 메모리 {orders.memory_usage(deep=True).sum() / 1e6:.0f} MB")

    spark = SparkSession.builder.appName("benchmark-engines").master("local[*]").getOrCreate()
    try:
        schema = (
            "order_id string, brand_customer_id string, store_id string, "
            "order_ts timestamp, category string, amount int"
        )
        started = time.perf_counter()
        (
            spark.read.option("header", True)
            .schema(schema)
            .csv(str(args.orders))
            .groupBy("store_id")
            .agg(F.count("*").alias("n"))
            .orderBy(F.desc("n"))
            .show(5)
        )
        print(f"[INFO] Spark 읽기 및 집계 {time.perf_counter() - started:.2f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
