"""주문 집계의 실행 계획과 셔플 파티션별 처리 시간을 확인한다."""

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DATA_ROOT = Path(os.environ.get("ORDERS_DATA_ROOT", PROJECT_ROOT / "data"))

    RAW_DIR = sys.argv[1] if len(sys.argv) > 1 else str(DATA_ROOT / "raw")

    spark = SparkSession.builder.appName("explore-orders").master("local[*]").getOrCreate()

    orders = (
        spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW_DIR}/orders")
    )  # brand=, dt= 폴더가 파티션 컬럼으로 읽힌다

    # 지연 실행: 여기까지는 아무것도 계산하지 않는다
    print("읽기 계획만 세움, 파티션 수:", orders.rdd.getNumPartitions())

    # 셔플이 없는 연산 (파티션 안에서 끝남)
    t = time.time()
    print("전체 건수:", orders.count(), f"({time.time() - t:.1f}s)")

    # 셔플이 생기는 연산 (같은 키를 한 파티션으로 모아야 함)
    by_store = orders.groupBy("store_id").agg(
        F.count("*").alias("n"), F.sum("amount").alias("sales")
    )
    by_store.explain()  # 실행 계획에서 Exchange hashpartitioning = 셔플
    t = time.time()
    by_store.orderBy(F.desc("n")).show(5)
    print(
        f"매장별 집계 {time.time() - t:.1f}s, 셔플 파티션 설정:",
        spark.conf.get("spark.sql.shuffle.partitions"),
    )

    # 셔플 파티션 수를 바꿔 같은 집계를 다시 잰다
    for n in [200, 16, 4]:
        spark.conf.set("spark.sql.shuffle.partitions", n)
        t = time.time()
        orders.groupBy("brand", "dt").agg(F.sum("amount")).collect()
        print(f"shuffle.partitions={n}: {time.time() - t:.1f}s")

    if sys.stdin.isatty():
        input("Spark UI(localhost:4040)를 확인한 뒤 Enter를 누르면 종료합니다...")
    spark.stop()


if __name__ == "__main__":
    main()
