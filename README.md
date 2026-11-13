# Data Engineering

Spark 배치 처리와 PostgreSQL CDC 수집을 로컬에서 구현한 개인 프로젝트다. 데이터 적재부터 분석 테이블 생성, 정합성 검사와 장애 복구까지 다룬다.

| 폴더 | 작업 |
|---|---|
| [batch-orders](batch-orders) | 주문 배치 처리, 일간 집계, 조인 성능 비교, Airflow 실행 |
| [cdc-reviews](cdc-reviews) | Debezium과 Kafka 수집, 리뷰 정제, DLT 복구, 배치 수집 비교 |
| [examples/docker-service](examples/docker-service) | Docker 컨테이너 실행 예제 |

처음에는 저장소 루트에서 `python scripts/init_env.py`로 로컬 설정을 만든 뒤 각 폴더의 README 순서대로 실행한다. Spark 배치는 Docker Compose에서, CDC 스크립트는 Python 가상환경에서 실행한다.

검증은 `python -m pip install -r requirements-dev.txt` 이후 `python -m pytest`와 `python -m ruff check .`로 실행한다.
