"""주문 팩트, 고객 디멘전과 일간 구매 집계를 만든다."""

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def main():
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DATA_ROOT = Path(os.environ.get("ORDERS_DATA_ROOT", PROJECT_ROOT / "data"))

    RAW_DIR = sys.argv[1] if len(sys.argv) > 1 else str(DATA_ROOT / "raw")
    CURATED_DIR = sys.argv[2] if len(sys.argv) > 2 else str(DATA_ROOT / "curated")

    spark = SparkSession.builder.appName("build-order-mart").master("local[*]").getOrCreate()

    order_schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("brand_customer_id", StringType()),
            StructField("store_id", StringType()),
            StructField("order_ts", TimestampType()),
            StructField("category", StringType()),
            StructField("amount", IntegerType()),
        ]
    )
    customer_schema = StructType(
        [
            StructField("brand_customer_id", StringType()),
            StructField("phone", StringType()),
            StructField("membership_no", StringType()),  # 앞자리 0이 있어 문자열로 읽는다
            StructField("joined_at", DateType()),
        ]
    )

    def timed(label, fn):
        t = time.time()
        result = fn()
        print(f"{label}: {time.time() - t:.1f}s")
        return result

    # 원천 읽기. brand=, dt= 폴더 이름이 컬럼이 된다
    orders = spark.read.option("header", True).schema(order_schema).csv(f"{RAW_DIR}/orders")
    customers = (
        spark.read.option("header", True).schema(customer_schema).csv(f"{RAW_DIR}/customers")
    )

    # 그레인 확인: order_id가 정말 주문 1건당 1개인가
    total, distinct = orders.agg(F.count("*"), F.countDistinct("order_id")).first()
    print(
        f"======= [CHECK] 주문 {total:,}행, order_id 고유 {distinct:,}개, 중복 {total - distinct:,}건"
    )

    # 팩트: dt로 파티션을 나눠 저장
    order_fact = orders.select(
        "order_id", "brand", "brand_customer_id", "store_id", "category", "amount", "order_ts", "dt"
    )
    timed(
        "fact_orders 쓰기",
        lambda: (
            order_fact.write.mode("overwrite")
            .partitionBy("dt")
            .parquet(f"{CURATED_DIR}/fact_orders")
        ),
    )

    # 디멘전
    timed(
        "dim_customer 쓰기",
        lambda: customers.write.mode("overwrite").parquet(f"{CURATED_DIR}/dim_customer"),
    )

    # 일간 활동 테이블: ukey x 날짜 한 줄. 저장은 쓴 결과를 다시 읽어서 만든다
    saved_orders = spark.read.parquet(f"{CURATED_DIR}/fact_orders")
    daily = (
        saved_orders.withColumn("ukey", F.concat_ws(":", "brand", "brand_customer_id"))
        .withColumn("activation_date", F.to_date("order_ts"))
        .groupBy("ukey", "brand", "activation_date")
        .agg(F.count("*").alias("order_cnt"), F.sum("amount").alias("amount_sum"))
    )

    timed(
        "f_purchase__ukey__daily 쓰기",
        lambda: (
            daily.drop("brand")
            .write.mode("overwrite")
            .partitionBy("activation_date")
            .parquet(f"{CURATED_DIR}/f_purchase__ukey__daily")
        ),
    )

    for b in ["food", "beauty", "cinema"]:
        timed(
            f"f_purchase_{b}__ukey__daily 쓰기",
            lambda b=b: (
                daily.filter(F.col("brand") == b)
                .drop("brand")
                .write.mode("overwrite")
                .partitionBy("activation_date")
                .parquet(f"{CURATED_DIR}/f_purchase_{b}__ukey__daily")
            ),
        )

    for i in range(1, 4):
        t = time.time()
        top = (
            spark.read.parquet(f"{CURATED_DIR}/fact_orders")
            .groupBy("store_id")
            .count()
            .orderBy(F.desc("count"))
            .limit(5)
            .collect()
        )
        print(f"Parquet 매장별 집계 {i}회차: {time.time() - t:.1f}s", top[0])

    # 일간 테이블 검증: 합계가 팩트와 같아야 한다
    fact_sum = saved_orders.agg(F.sum("amount")).first()[0]
    daily_sum = (
        spark.read.parquet(f"{CURATED_DIR}/f_purchase__ukey__daily")
        .agg(F.sum("amount_sum"))
        .first()[0]
    )
    print(
        f"======= [CHECK] 금액 합계 팩트 {fact_sum:,} / 일간 테이블 {daily_sum:,} / 일치: {fact_sum == daily_sum}"
    )

    if sys.stdin.isatty():
        input("Spark UI(localhost:4040)를 확인한 뒤 Enter를 누르면 종료합니다...")
    spark.stop()


if __name__ == "__main__":
    main()
