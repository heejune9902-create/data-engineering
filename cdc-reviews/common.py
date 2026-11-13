"""공통 연결 설정과 리뷰 정제 규칙."""

import hashlib
import hmac
import os
import re
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")
LOG_DIR = PROJECT_ROOT / "logs"
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
SRC_DSN = os.environ.get("SOURCE_DSN", "host=localhost port=15432 dbname=shop user=demo")
DW_DSN = os.environ.get("WAREHOUSE_DSN", "host=localhost port=15432 dbname=dw user=demo")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "oy.public.reviews"
# 디멘션용: 상품과 고객 변경도 CDC로 받아 d_product, d_customer를 갱신한다
PRODUCT_TOPIC = "oy.public.products"
CUSTOMER_TOPIC = "oy.public.customers"
DLT_TOPIC = "oy.reviews.dlt"
GROUP = "dw-loader"

PSEUDO_KEY = os.environ["PSEUDONYM_KEY"].encode("utf-8")


def src():
    return psycopg2.connect(SRC_DSN, password=POSTGRES_PASSWORD)


def dw():
    return psycopg2.connect(DW_DSN, password=POSTGRES_PASSWORD)


def pseudonym(customer_id):
    return hmac.new(PSEUDO_KEY, str(customer_id).encode(), hashlib.sha256).hexdigest()[:16]


def content_hash(body):
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


PHONE = re.compile(r"01[016789][-\s.]?\d{3,4}[-\s.]?\d{4}")


def mask(body):
    masked, n = PHONE.subn("[전화번호]", body)
    return masked, n > 0


def normalize(body):
    return re.sub(r"[\s\W_]+", "", body)


POS = ("좋아", "좋네", "좋습니다", "만족", "최고", "추천", "굿", "예뻐", "이뻐", "촉촉", "괜찮")
NEG = ("별로", "실망", "최악", "불편", "환불", "아쉽", "비추", "트러블", "뒤집", "따가")
DELIVERY = re.compile(r"배송|택배")


def flags(body, rating, is_duplicate, pii):
    """d_review_flag의 flag_key를 계산한다. 지우지 않고 표시만 한다."""
    has_pos = any(w in body for w in POS)
    has_neg = any(w in body for w in NEG)
    mismatch = (rating <= 2 and has_pos and not has_neg) or (
        rating >= 4 and has_neg and not has_pos
    )
    rest = "".join(s for s in re.split(r"[.!?\n]", body) if not DELIVERY.search(s))
    delivery_only = bool(DELIVERY.search(body)) and len(normalize(rest)) < 5
    return (
        (1 if is_duplicate else 0)
        | (2 if delivery_only else 0)
        | (4 if mismatch else 0)
        | (8 if pii else 0)
    )
