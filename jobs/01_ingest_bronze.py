"""
JOB 01 — Ingestion: CSV thô (landing zone) -> tầng BRONZE trong Data Lake.

- Đọc CSV với schema khai báo sẵn (không inferSchema).
- KHÔNG làm sạch ở bước này: bronze giữ nguyên dữ liệu gốc để có thể xử lý lại (replay).
- Thêm metadata nguồn: _source_file, _ingested_at.
- Ghi Parquet, phân vùng theo event_date -> truy vấn theo ngày chỉ đọc đúng thư mục cần thiết.

Chạy:  make ingest RAW="/data/raw/kaggle/*.csv"
"""
import argparse

from pyspark.sql import functions as F

from common import RAW_DIR, RAW_SCHEMA, ensure_bucket, get_spark, lake, timed

JOB = "01_ingest_bronze"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=f"{RAW_DIR}/sample/*.csv", help="glob file CSV nguồn")
    args = ap.parse_args()

    spark = get_spark(JOB)
    ensure_bucket()
    out = lake("bronze", "events")

    raw = (spark.read
           .option("header", True)
           .option("mode", "PERMISSIVE")          # dòng lỗi -> null, để tầng silver đếm và loại
           .schema(RAW_SCHEMA)
           .csv(args.input))

    bronze = (raw
              .withColumn("event_date", F.to_date(F.substring("event_time", 1, 10)))
              .withColumn("_source_file", F.input_file_name())
              .withColumn("_ingested_at", F.current_timestamp()))

    with timed(spark, JOB, "write_bronze_parquet", {"input": args.input}):
        (bronze
         .repartition("event_date")               # ~1 file/ngày, tránh quá nhiều file nhỏ
         .write.mode("overwrite")                 # dynamic: chỉ ghi đè các ngày có trong input
         .partitionBy("event_date")
         .parquet(out))

    with timed(spark, JOB, "verify_count") as info:
        stored = spark.read.parquet(out)
        info["rows"] = stored.count()

    print("\n=== Bronze: số bản ghi theo ngày (10 ngày đầu) ===")
    stored.groupBy("event_date").count().orderBy("event_date").show(10, truncate=False)
    stored.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
