"""
JOB 05 — Thực nghiệm hiệu năng. Chạy cùng 1 bộ truy vấn trên các cấu hình khác nhau và ghi
kết quả ra output/benchmarks/results.csv để vẽ biểu đồ trong báo cáo.

Các trục thí nghiệm (đổi qua tham số / --conf của spark-submit):
  --format   csv | bronze | silver         -> CSV thô so với Parquet (phân vùng)
             bronze_stream                 -> đếm dữ liệu do streaming ghi (kiểm chứng chịu lỗi)
  --date-from/--date-to                    -> khối lượng dữ liệu (1 tuần / 2 tuần / cả tháng)
  --conf spark.cores.max=1|2|4             -> khả năng mở rộng theo số core (speedup)
  --conf spark.sql.shuffle.partitions=N    -> ảnh hưởng số partition shuffle

Chạy:  make bench FORMAT=silver CORES=4
"""
import argparse
import os
import statistics
import time
from datetime import datetime

from pyspark.sql import functions as F

from common import (OUTPUT_DIR, RAW_DIR, RAW_SCHEMA, append_csv, cluster_info,
                    get_spark, lake)

JOB = "05_benchmark"


def load(spark, fmt: str, raw_glob: str):
    if fmt == "csv":
        df = (spark.read.option("header", True).schema(RAW_SCHEMA).csv(raw_glob)
              .withColumn("event_date", F.to_date(F.substring("event_time", 1, 10))))
    else:
        df = spark.read.parquet(lake(fmt, "events"))
    return df


QUERIES = {
    "Q1_count": lambda df: [df.count()],
    "Q2_daily_revenue": lambda df: (df.filter(F.col("event_type") == "purchase")
                                    .groupBy("event_date").agg(F.sum("price")).collect()),
    "Q3_top_brands": lambda df: (df.filter(F.col("event_type") == "purchase")
                                 .groupBy("brand").agg(F.sum("price").alias("rev"))
                                 .orderBy(F.desc("rev")).limit(10).collect()),
    "Q4_distinct_users": lambda df: [df.select("user_id").distinct().count()],
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--format", choices=["csv", "bronze", "silver", "bronze_stream"], default="silver")
    ap.add_argument("--raw", default=f"{RAW_DIR}/kaggle/*.csv", help="glob CSV khi --format csv")
    ap.add_argument("--date-from", default=None)
    ap.add_argument("--date-to", default=None)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--tag", default="", help="nhãn thí nghiệm, vd: scale_cores")
    args = ap.parse_args()

    spark = get_spark(f"{JOB}_{args.format}")
    df = load(spark, args.format, args.raw)
    if args.date_from:
        df = df.filter(F.col("event_date") >= F.lit(args.date_from))
    if args.date_to:
        df = df.filter(F.col("event_date") <= F.lit(args.date_to))

    info = cluster_info(spark)
    queries = dict(QUERIES)
    if args.format == "bronze_stream":
        # Kiểm chứng chịu lỗi: số offset Kafka phân biệt so với số dòng (trùng do ghi lại sau sự cố)
        queries["Q5_distinct_offsets"] = lambda df: [df.select("partition", "offset").distinct().count()]
    for qname, q in queries.items():
        times = []
        for i in range(args.repeat):
            t0 = time.perf_counter()
            res = q(df)
            times.append(time.perf_counter() - t0)
            append_csv(os.path.join(OUTPUT_DIR, "benchmarks", "results.csv"), {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "tag": args.tag, "format": args.format,
                "date_from": args.date_from or "all", "date_to": args.date_to or "all",
                "query": qname, "run": i + 1, "seconds": round(times[-1], 3),
                "result_rows": len(res), **info,
            })
        if qname in ("Q1_count", "Q5_distinct_offsets", "Q4_distinct_users"):
            print(f"{qname:20s} kết quả = {res[0]:,}")
        print(f"{qname:20s} median={statistics.median(times):7.2f}s  runs={[round(t, 2) for t in times]}")

    spark.stop()


if __name__ == "__main__":
    main()
