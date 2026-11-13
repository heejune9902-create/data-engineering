-- 리뷰 원본 DB
CREATE TABLE products (
    product_id  int PRIMARY KEY,
    brand       text NOT NULL,
    category    text NOT NULL,
    price_band  text NOT NULL
);

-- 이름과 연락처는 원본에만 있고 분석 DB로 넘어가지 않는다
CREATE TABLE customers (
    customer_id int PRIMARY KEY,
    name        text NOT NULL,
    phone       text NOT NULL,
    skin_type   text NOT NULL,
    age_band    text NOT NULL,
    grade       text NOT NULL
);

CREATE TABLE reviews (
    review_id   bigint PRIMARY KEY,
    product_id  int NOT NULL REFERENCES products,
    customer_id int NOT NULL REFERENCES customers,
    rating      smallint NOT NULL,
    body        text NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);
-- 삭제 이벤트에도 이전 값을 싣기 위해
ALTER TABLE reviews REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
CREATE SEQUENCE review_id_seq START 300000;

-- 분석 DB
CREATE DATABASE dw;
