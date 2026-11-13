import json
from datetime import datetime, timezone

import pytest
from common import content_hash, flags, mask, normalize, pseudonym
from consumer import parse


class Message:
    def __init__(self, payload):
        self.payload = payload

    def value(self):
        return json.dumps(self.payload, ensure_ascii=False).encode("utf-8")

    def partition(self):
        return 2

    def offset(self):
        return 41


def review_event(operation="u"):
    row = {
        "review_id": 7,
        "body": "촉촉하고 좋아요",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    return {
        "op": operation,
        "before": dict(row, body="수정 전 본문"),
        "after": None if operation == "d" else row,
        "source": {"ts_ms": 1767225601000, "lsn": 123},
    }


@pytest.mark.parametrize("phone", ["010-1234-5678", "01012345678", "010 1234 5678"])
def test_phone_is_removed_from_review_text(phone):
    text, found = mask(f"문의는 {phone}로 주세요")
    assert found
    assert phone not in text
    assert "[전화번호]" in text


def test_plain_review_is_kept():
    assert mask("촉촉하고 좋아요") == ("촉촉하고 좋아요", False)


def test_quality_flags_can_coexist():
    assert flags("좋아요", 1, True, True) == 1 | 4 | 8
    assert flags("배송 빨라요", 5, False, False) == 2


def test_customer_key_is_stable_and_keyed(monkeypatch):
    import common

    first = pseudonym(123)
    assert first == pseudonym("123")
    assert first != pseudonym(124)
    monkeypatch.setattr(common, "PSEUDO_KEY", b"another-unused-test-key")
    assert first != pseudonym(123)


def test_duplicate_comparison_ignores_spacing_and_punctuation():
    assert content_hash(normalize("정말 좋아요!")) == content_hash(normalize("정말좋아요"))


def test_update_keeps_offsets_and_separate_content_hashes():
    event = parse(Message(review_event()), set())
    assert (event["partition"], event["offset"], event["lsn"]) == (2, 41, 123)
    assert event["old_hash"] != event["new_hash"]
    assert event["version"] == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_delete_uses_source_time_and_before_row():
    event = parse(Message(review_event("d")), {7})
    assert event["review_id"] == 7
    assert event["new_hash"] is None
    assert event["version"] == event["source_ts"]


def test_injected_failure_is_reported_for_live_review():
    with pytest.raises(ValueError, match="injected processing failure"):
        parse(Message(review_event()), {7})
