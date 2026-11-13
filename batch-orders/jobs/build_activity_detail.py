"""고객별 활동 순서와 재방문 간격을 계산한다."""

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DATA_ROOT = Path(os.environ.get("ORDERS_DATA_ROOT", PROJECT_ROOT / "data"))

    CURATED_DIR = sys.argv[1] if len(sys.argv) > 1 else str(DATA_ROOT / "curated")

    spark = SparkSession.builder.appName("build-activity-detail").master("local[*]").getOrCreate()

    daily = spark.read.parquet(f"{CURATED_DIR}/f_purchase__ukey__daily")

    # 그레인 확인: ukey x 날짜가 중복 없이 1행이어야 앞뒤 비교가 의미 있다
    total = daily.count()
    distinct = daily.select("ukey", "activation_date").distinct().count()
    print(
        f"======= [CHECK] 일간 테이블 {total:,}행, ukey x 날짜 고유 {distinct:,}개, 중복 {total - distinct:,}건"
    )

    # 윈도 함수: ukey별로 활동일을 시간순으로 줄 세운다
    w = Window.partitionBy("ukey").orderBy("activation_date")
    detail = (
        daily.withColumn("seq", F.row_number().over(w))
        .withColumn("prev_activation_date", F.lag("activation_date").over(w))
        .withColumn("next_activation_date", F.lead("activation_date").over(w))
        .withColumn("retain_days", F.datediff("activation_date", "prev_activation_date"))
        .withColumn("churn_days", F.datediff("next_activation_date", "activation_date"))
        .withColumn("is_last", F.col("next_activation_date").isNull())
        .select(
            "ukey",
            "activation_date",
            "seq",
            "prev_activation_date",
            "retain_days",
            "next_activation_date",
            "churn_days",
            "is_last",
            "order_cnt",
            "amount_sum",
        )
    )

    detail.explain()  # Window 앞의 Exchange hashpartitioning(ukey, ...) = 셔플, 그 뒤 Sort

    t = time.time()
    detail.write.mode("overwrite").partitionBy("activation_date").parquet(
        f"{CURATED_DIR}/detail_daily"
    )
    print(f"detail_daily 쓰기: {time.time() - t:.1f}s")

    # 검증: 행 수는 입력과 같아야 하고, 사람마다 seq의 최댓값은 그 사람의 활동일 수와 같아야 한다
    d = spark.read.parquet(f"{CURATED_DIR}/detail_daily")
    n_detail = d.count()
    bad = (
        d.groupBy("ukey")
        .agg(F.max("seq").alias("mx"), F.count("*").alias("n"))
        .filter("mx != n")
        .count()
    )
    last_cnt = d.filter("is_last").count()
    ukeys = d.select("ukey").distinct().count()
    print(
        f"======= [CHECK] detail_daily {n_detail:,}행 (입력과 일치: {n_detail == total}), seq 불일치 ukey {bad}개"
    )
    print(f"======= [CHECK] is_last {last_cnt:,}행, ukey {ukeys:,}개, 일치: {last_cnt == ukeys}")

    # 신규, 복귀, 이탈을 쿼리 한 줄씩으로 센다. 기준 일수를 바꿔 본다
    d.createOrReplaceTempView("detail_daily")
    print(
        "신규(seq=1) 행:", spark.sql("SELECT count(*) FROM detail_daily WHERE seq = 1").first()[0]
    )

    # seq=1이 날짜별로 어떻게 퍼져 있나: 첫날에 몰렸으면 기간 이전부터 있던 사람이 섞인 것이다
    new_by_day = spark.sql("""SELECT activation_date, count(*) AS n FROM detail_daily
                              WHERE seq = 1 GROUP BY activation_date ORDER BY activation_date""").collect()
    new_total = sum(r.n for r in new_by_day)
    rest = new_by_day[7:]
    print(
        f"신규 날짜별 첫날 {new_by_day[0].activation_date}: {new_by_day[0].n:,}건 ({new_by_day[0].n / new_total:.1%})"
    )
    print("신규 날짜별 처음 7일:", [(str(r.activation_date), r.n) for r in new_by_day[:7]])
    print(
        f"신규 날짜별 8일째 이후 하루 평균: {sum(r.n for r in rest) / len(rest) if rest else 0:,.0f}건 ({len(new_by_day)}일 중 {len(rest)}일)"
    )
    for n in [30, 14, 7]:
        ret = spark.sql(f"SELECT count(*) FROM detail_daily WHERE retain_days >= {n}").first()[0]
        chn = spark.sql(f"SELECT count(*) FROM detail_daily WHERE churn_days >= {n}").first()[0]
        print(f"기준 {n}일: 복귀 {ret:,}건, 이탈 {chn:,}건")

    # 데이터 기간의 양 끝을 확인한다 (신규와 이탈 해석에 필요)
    lo, hi = d.agg(F.min("activation_date"), F.max("activation_date")).first()
    print(f"활동일 범위: {lo} ~ {hi}")

    wait = int(os.environ.get("UI_WAIT", "0"))  # 터미널이 아닐 때 UI를 열어 둘 초
    if sys.stdin.isatty():
        input("Spark UI(localhost:4040)를 확인한 뒤 Enter를 누르면 종료합니다...")
    elif wait:
        print(f"Spark UI(localhost:4040)를 {wait}초 동안 열어 둡니다...")
        time.sleep(wait)
    spark.stop()


if __name__ == "__main__":
    main()
