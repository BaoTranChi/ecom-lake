"""
JOB 02 — Processing: BRONZE -> SILVER (làm sạch, chuẩn hóa) + báo cáo chất lượng dữ liệu.

Luật làm sạch (mỗi luật được đếm số dòng vi phạm để đưa vào báo cáo) — xem jobs/transforms.py:
  R1  thiếu khóa: user_id / product_id / event_time / event_type null, hoặc event_time sai định dạng
  R2  event_type ngoài tập {view, cart, remove_from_cart, purchase}
  R3  price null hoặc <= 0
  R4  bản ghi trùng lặp (cùng thời điểm, user, session, product, event_type)
Chuẩn hóa (không loại dòng): brand lower/trim, null -> 'unknown'; tách category_code thành
cat_l1/cat_l2; thêm event_hour, day_of_week.

Chạy:  make clean
"""
import json
import os
from datetime import datetime

from common import OUTPUT_DIR, get_spark, lake, timed
from transforms import add_quality_flags, clean_events, quality_profile

JOB = "02_clean_silver"


def main():
    spark = get_spark(JOB)
    # Bỏ cột metadata (_source_file là chuỗi đường dẫn dài lặp lại trên MỌI dòng) ngay khi đọc.
    bronze = spark.read.parquet(lake("bronze", "events")).drop("_source_file", "_ingested_at")

    # KHÔNG persist: với 42 triệu dòng, giữ tập đã gắn cờ trong RAM làm executor 1,4 GB bị
    # OutOfMemoryError. Đọc lại Parquet lần 2 rẻ hơn (chỉ đọc cột cần, có nén) -> 2 lượt quét độc lập.
    flagged = add_quality_flags(bronze)

    with timed(spark, JOB, "quality_profile") as info:
        q = quality_profile(flagged)
        info["rows"] = q["total_rows"]

    with timed(spark, JOB, "write_silver_parquet"):
        (clean_events(flagged)
         .repartition("event_date")
         .write.mode("overwrite").partitionBy("event_date")
         .parquet(lake("silver", "events")))

    with timed(spark, JOB, "verify_count") as info:
        silver = spark.read.parquet(lake("silver", "events"))
        silver_rows = silver.count()
        info["rows"] = silver_rows

    valid_rows = q["total_rows"] - (q["invalid_rows_r1_r3"] or 0)
    q.update({
        "rows_after_rules_r1_r3": valid_rows,
        "r4_duplicates_removed": valid_rows - silver_rows,
        "silver_rows": silver_rows,
        "kept_ratio": round(silver_rows / q["total_rows"], 6) if q["total_rows"] else None,
        "null_brand_ratio": round((q["null_brand"] or 0) / q["total_rows"], 4) if q["total_rows"] else None,
        "null_category_ratio": round((q["null_category_code"] or 0) / q["total_rows"], 4) if q["total_rows"] else None,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    })

    path = os.path.join(OUTPUT_DIR, "quality", "quality_report.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(q, f, indent=2, ensure_ascii=False)

    print("\n=== BÁO CÁO CHẤT LƯỢNG DỮ LIỆU ===")
    print(json.dumps(q, indent=2, ensure_ascii=False))
    print("\n=== Mẫu dữ liệu TRƯỚC xử lý (bronze, các dòng bị loại) ===")
    (flagged.filter("f_invalid").limit(5)
     .select("event_time", "event_type", "product_id", "brand", "price",
             "user_id", "f_missing_key", "f_bad_type", "f_bad_price").show(5, truncate=False))
    print("=== Mẫu dữ liệu SAU xử lý (silver) ===")
    silver.show(5, truncate=False)
    silver.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
