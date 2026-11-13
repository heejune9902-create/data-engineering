"""조인 방식, salting과 AQE의 처리 시간을 비교한다."""

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DATA_ROOT = Path(os.environ.get("ORDERS_DATA_ROOT", PROJECT_ROOT / "data"))

    CURATED_DIR = sys.argv[1] if len(sys.argv) > 1 else str(DATA_ROOT / "curated")
    REPEATS = 3
    SALT_BUCKETS = 8

    spark = SparkSession.builder.appName("benchmark-joins").master("local[*]").getOrCreate()

    order_fact = spark.read.parquet(f"{CURATED_DIR}/fact_orders")
    customers = spark.read.parquet(f"{CURATED_DIR}/dim_customer")
    keys = ["brand", "brand_customer_id"]

    # 공정한 비교를 위해 자동 최적화를 끈다. 지정한 방식만 쓰이게 한다
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    n_fact = order_fact.count()  # 처음 읽는 비용을 첫 측정에서 빼기 위한 예열이기도 하다
    print(f"fact_orders {n_fact:,}행, dim_customer {customers.count():,}행")

    def run(tag, label, build):
        """build()가 만든 DataFrame을 REPEAT번 끝까지 계산하고 시간을 출력한다."""
        times = []
        for _ in range(REPEATS):
            df = build()
            t = time.time()
            df.write.format("noop").mode("overwrite").save()
            times.append(time.time() - t)
        shown = ", ".join(f"{x:.1f}s" for x in times)
        print(f"[{tag}] {label}: {shown} (평균 {sum(times) / len(times):.1f}s)")
        return times

    def join_kind(df):
        plan = df._jdf.queryExecution().executedPlan().toString()
        if "BroadcastHashJoin" in plan:
            return "BroadcastHashJoin"
        if "SortMergeJoin" in plan:
            return "SortMergeJoin"
        return "기타"

    # 실험 1: 고객 디멘전 join. 방식 x 셔플 파티션 수
    builders = {
        "broadcast": lambda: order_fact.join(F.broadcast(customers), keys),
        "sort-merge": lambda: order_fact.join(customers.hint("merge"), keys),
    }
    for name, build in builders.items():
        print(f"[실험1] {name} 실제 계획: {join_kind(build())}")
    n_joined = builders["broadcast"]().count()
    print(f"======= [CHECK] [실험1] join 결과 {n_joined:,}행 (팩트와 일치: {n_joined == n_fact})")
    for name, build in builders.items():
        for n in [200, 16, 4]:
            spark.conf.set("spark.sql.shuffle.partitions", n)
            run("실험1", f"{name}, 셔플 파티션 {n}", build)

    # 실험 2: 매장 키로 나눌 때 파티션별 행 수. 최대 대 중앙값이 쏠림 정도다
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    salted_orders = order_fact.withColumn("salt", (F.rand(42) * SALT_BUCKETS).cast("int"))

    def partition_stats(df, cols, label):
        rows = (
            df.repartition(200, *cols).groupBy(F.spark_partition_id().alias("p")).count().collect()
        )
        counts = sorted(r["count"] for r in rows)
        mid = counts[len(counts) // 2]
        print(
            f"[실험2] {label}: 비어 있지 않은 파티션 {len(counts)}개, 최대 {counts[-1]:,}행, "
            f"중앙값 {mid:,}행, 최대/중앙값 {counts[-1] / mid:.1f}배"
        )

    partition_stats(order_fact, ["store_id"], "store_id만")
    partition_stats(salted_orders, ["store_id", "salt"], f"store_id + salt({SALT_BUCKETS})")

    # 실험 3: 매장 디멘전(300행) join 4가지
    dim_store = spark.range(1, 301).select(
        F.format_string("S%03d", F.col("id")).alias("store_id"), (F.col("id") % 5).alias("region")
    )
    dim_store_salt = dim_store.crossJoin(
        spark.range(SALT_BUCKETS).select(F.col("id").cast("int").alias("salt"))
    )

    spark.conf.set("spark.sql.shuffle.partitions", 200)
    run(
        "실험3", "sort-merge (AQE 끔)", lambda: order_fact.join(dim_store.hint("merge"), "store_id")
    )
    run(
        "실험3",
        f"salting {SALT_BUCKETS} (AQE 끔)",
        lambda: salted_orders.join(dim_store_salt.hint("merge"), ["store_id", "salt"]),
    )
    run("실험3", "broadcast (AQE 끔)", lambda: order_fact.join(F.broadcast(dim_store), "store_id"))

    # AQE skew join: 이 데이터가 작아서 기본 임계값(256MB)으로는 작동하지 않으므로 낮춰 둔다
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "5MB")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4MB")
    run(
        "실험3",
        "sort-merge + AQE skew join (임계값 5MB로 낮춤)",
        lambda: order_fact.join(dim_store.hint("merge"), "store_id"),
    )

    wait = int(os.environ.get("UI_WAIT", "0"))  # 터미널이 아닐 때 UI를 열어 둘 초
    if sys.stdin.isatty():
        input("Spark UI(localhost:4040)를 확인한 뒤 Enter를 누르면 종료합니다...")
    elif wait:
        print(f"Spark UI(localhost:4040)를 {wait}초 동안 열어 둡니다...")
        time.sleep(wait)
    spark.stop()


if __name__ == "__main__":
    main()
