"""Kiểm thử luật làm sạch R1–R4 và chuẩn hóa silver."""
from common import RAW_SCHEMA
from transforms import SILVER_COLUMNS, add_quality_flags, clean_events, quality_profile

ROWS = [
    # event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
    ("2019-10-01 10:00:00 UTC", "view", 1, 10, "electronics.smartphone", "Samsung ", 100.0, 7, "s1"),  # hợp lệ
    ("2019-10-01 10:00:00 UTC", "view", 1, 10, "electronics.smartphone", "Samsung ", 100.0, 7, "s1"),  # R4 trùng
    ("2019-10-01 10:05:00 UTC", "purchase", 2, 11, None, None, 50.0, 7, "s1"),                        # hợp lệ, thiếu brand/cat
    ("2019-10-01 10:06:00 UTC", "view", 3, 12, "kids.toys", "lego", 20.0, None, "s2"),                # R1 thiếu user
    ("not-a-date", "view", 3, 12, "kids.toys", "lego", 20.0, 8, "s2"),                                 # R1 thời gian sai
    ("2019-10-01 10:07:00 UTC", "VIEWED", 3, 12, "kids.toys", "lego", 20.0, 8, "s2"),                  # R2
    ("2019-10-01 10:08:00 UTC", "cart", 4, 13, "kids.toys", "lego", 0.0, 8, "s2"),                     # R3
    ("2019-10-01 10:09:00 UTC", "Cart", 5, 13, "kids.toys", "", 15.0, 8, "s2"),                        # hợp lệ (Cart -> cart)
]


def _flagged(spark):
    return add_quality_flags(spark.createDataFrame(ROWS, RAW_SCHEMA))


def test_quality_profile_counts(spark):
    q = quality_profile(_flagged(spark))
    assert q["total_rows"] == 8
    assert q["r1_missing_key"] == 2
    assert q["r3_bad_price"] == 1
    assert q["invalid_rows_r1_r3"] == 4


def test_clean_removes_invalid_and_duplicates(spark):
    out = clean_events(_flagged(spark))
    assert out.columns == SILVER_COLUMNS
    assert out.count() == 3          # 8 - 4 vi phạm - 1 trùng


def test_normalization(spark):
    rows = {r["product_id"]: r for r in clean_events(_flagged(spark)).collect()}
    assert rows[1]["brand"] == "samsung"
    assert rows[1]["cat_l1"] == "electronics" and rows[1]["cat_l2"] == "smartphone"
    assert rows[2]["brand"] == "unknown" and rows[2]["category_code"] == "unknown"
    assert rows[5]["event_type"] == "cart" and rows[5]["brand"] == "unknown"
    assert rows[1]["event_hour"] == 10 and str(rows[1]["event_date"]) == "2019-10-01"
