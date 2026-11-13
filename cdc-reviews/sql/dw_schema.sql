-- 분석 DB (dw): 스타 스키마

-- 디멘션
CREATE TABLE d_product (
    product_key int PRIMARY KEY,
    brand       text NOT NULL,
    category    text NOT NULL,
    price_band  text NOT NULL
);

-- 가명 키와 구간 정보만 둔다
CREATE TABLE d_customer (
    customer_key text PRIMARY KEY,
    skin_type    text NOT NULL,
    age_band     text NOT NULL,
    grade        text NOT NULL
);

CREATE TABLE d_date (
    date_key   int PRIMARY KEY,
    full_date  date NOT NULL,
    weekday    text NOT NULL,
    is_sale    boolean NOT NULL
);

-- 품질 플래그 조합 (지우지 않고 표시만)
CREATE TABLE d_review_flag (
    flag_key          int PRIMARY KEY,
    is_duplicate      boolean NOT NULL,
    is_delivery_only  boolean NOT NULL,
    is_rating_mismatch boolean NOT NULL,
    has_pii_masked    boolean NOT NULL
);
INSERT INTO d_review_flag
SELECT k, (k & 1) > 0, (k & 2) > 0, (k & 4) > 0, (k & 8) > 0 FROM generate_series(0, 15) k;

-- 팩트: 변경 이벤트 1건 1행, 고치지 않고 쌓기만 한다
CREATE TABLE f_review_event (
    event_id        bigserial PRIMARY KEY,
    review_id       bigint NOT NULL,
    op              char(1) NOT NULL,
    source_ts       timestamptz NOT NULL,
    source_lsn      bigint,
    kafka_partition int NOT NULL,
    kafka_offset    bigint NOT NULL,
    old_hash        text,
    new_hash        text,
    ingested_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (kafka_partition, kafka_offset)
);
CREATE INDEX ON f_review_event (review_id);

-- 팩트: 리뷰 1건 1행, 현재 상태
CREATE TABLE f_review (
    review_id      bigint PRIMARY KEY,
    product_key    int,
    customer_key   text,
    date_key       int,
    flag_key       int REFERENCES d_review_flag,
    rating         smallint,
    text_len       int,
    content_hash   text,
    is_deleted     boolean NOT NULL DEFAULT false,
    src_version_ts timestamptz NOT NULL,
    applied_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- 정제 본문
CREATE TABLE s_review_text (
    review_id   bigint PRIMARY KEY,
    masked_text text NOT NULL,
    norm_hash   text NOT NULL
);
CREATE INDEX ON s_review_text (norm_hash);

-- 처리 실패 기록 (DLT 사본)
CREATE TABLE dlt_log (
    id          bigserial PRIMARY KEY,
    review_id   bigint,
    kafka_partition int,
    kafka_offset bigint,
    error       text,
    logged_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    recovered_at timestamptz
);

-- 컨슈머 실행 통계 (중복 흡수 건수 등)
CREATE TABLE consumer_stats (
    id          bigserial PRIMARY KEY,
    run_label   text,
    processed   bigint,
    applied     bigint,
    dup_events  bigint,
    stale_skipped bigint,
    dropped     bigint,
    sent_to_dlt bigint,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- 매일 점검 결과
CREATE TABLE dq_check (
    id             bigserial PRIMARY KEY,
    run_label      text,
    source_live    bigint,
    dw_live        bigint,
    missing        bigint,
    hash_mismatch  bigint,
    delete_unapplied bigint,
    recovered      bigint,
    unrecovered    bigint,
    checked_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);
