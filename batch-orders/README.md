# 주문 배치 파이프라인

브랜드별 주문을 팩트와 고객 디멘전으로 정리하고, 고객별 일간 구매와 활동 간격을 계산한다. 별도 벤치마크에서 셔플 파티션, broadcast join, salting과 AQE를 비교한다.

`batch-orders` 폴더에서 실행한다. `.env.example`을 `.env`로 복사하고 로컬 접속 토큰을 지정한다.

```bash
docker compose up -d
docker compose exec -T spark python scripts/generate_orders.py
docker compose exec -T spark spark-submit jobs/build_order_mart.py
docker compose exec -T spark spark-submit jobs/build_activity_detail.py
```

실행 계획과 성능 비교는 별도로 실행한다.

```bash
docker compose exec -T spark spark-submit jobs/explore_orders.py
docker compose exec -T spark spark-submit jobs/benchmark_joins.py
docker compose exec -T spark spark-submit jobs/benchmark_engines.py
```

생성 규모는 `generate_orders.py`의 `--people`, `--orders`, `--days`로 조절한다. 기본 출력 경로는 프로젝트의 `data` 폴더이며, 로컬 실행에서는 `ORDERS_DATA_ROOT`로 바꿀 수 있다. 배치 작업을 다시 실행하면 해당 출력 테이블을 전체 교체한다.

Airflow는 `airflow` 폴더에서 실행한다. `.env.example`을 `.env`로 복사한 뒤 `BATCH_PROJECT_DIR`에 이 폴더의 절대 경로를 지정한다. Windows 경로는 `C:/...` 형식으로 적는다. Docker 소켓 접근이 가능한 로컬 환경에서 사용한다.

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

`http://localhost:8080`에 로그인하고 `order_batch_pipeline` DAG를 실행한다. `build_order_mart` 이후 `build_activity_detail`이 실행된다. 주문 생성은 앞 단계에서 한 번 완료해 둔다.
