# 리뷰 CDC 파이프라인

PostgreSQL의 리뷰 변경을 Debezium과 Kafka로 수집해 분석 테이블에 반영한다. 중복 이벤트, 이전 버전 재전송, 처리 실패와 정합성 불일치를 확인하고 복구한다.

`cdc-reviews` 폴더에서 실행한다. Python 가상환경에 `requirements.txt`를 설치하고 `.env.example`을 `.env`로 복사한다. DB 비밀번호와 가명 처리 키를 지정한다. 가명 처리 키는 초기 적재 이후에도 같은 값을 사용한다.

1. `docker compose up -d`로 PostgreSQL, Kafka와 Connect를 시작한다. `docker compose logs connect`에서 기동 상태를 확인한다.
2. `python load_data.py --reviews <리뷰 TSV 경로>`로 최초 적재한다. 기존 환경에서 반복 적재하지 않는다.
3. `python register_connector.py`로 커넥터를 등록한다. `http://localhost:8083/connectors/reviews-cdc/status`에서 커넥터와 태스크의 `RUNNING` 상태를 확인한다.
4. `python consumer.py --label snapshot --idle-exit 30`으로 초기 스냅샷을 반영한다.
5. `python daily_check.py --no-repair`로 불일치가 없는지 확인한다. 스냅샷이 진행 중이면 4번을 다시 실행한다.
6. `python run_scenarios.py`로 장애 및 복구 시나리오를 실행한다. 결과는 `logs/results.json`에 저장된다.
7. `python run_queries.py`로 분석 쿼리를 실행한다.

일반 수집은 `python consumer.py`, 정합성 검사 및 복구는 `python daily_check.py`, DLT 복구는 `python recover_dlt.py`로 실행한다. `daily_check.py --no-repair`는 검사만 한다.

추가 비교는 기본 시나리오 종료 후 각각 실행한다. 다른 컨슈머가 실행 중이면 먼저 종료한다.

```bash
python overload.py
bash batch_compare/run.sh
python batch_compare/analyze.py
```

`batch_compare/run.sh`는 Git Bash 또는 Bash 환경에서 실행한다. `PYTHON` 환경변수로 사용할 Python 실행 파일을 지정할 수 있다. 비교 결과는 `batch_compare/result.md`에 생성된다. 증분 배치는 `updated_at`으로 조회하므로 삭제를 직접 감지하지 못한다.
