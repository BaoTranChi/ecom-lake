"""
JOB 06 — Streaming (Chương 6): Kafka `ecom.events` -> Spark Structured Streaming.

Hai truy vấn chạy song song trên cùng một nguồn:
  [A] events_to_lake  : lọc sự kiện hợp lệ, ghi append vào s3a://lake/bronze_stream/events
                        (Parquet, partition event_date) + đo ĐỘ TRỄ end-to-end
                        producer -> Kafka -> Spark -> Data Lake cho từng micro-batch.
  [B] kpi_windows     : gom theo cửa sổ thời gian sự kiện (tumbling window) có watermark,
                        output mode 'append' -> mỗi cửa sổ phát ĐÚNG 1 LẦN khi đã chốt;
                        phát KPI sang Kafka `ecom.kpi` (cho rule engine), ghi lake + CSV.
Driver ghi lại lastProgress (input rate, processed rate, thời gian trigger) mỗi 10 giây.

Chạy:  make stream            (Ctrl+C để dừng; checkpoint giúp chạy lại không mất/không trùng offset)
"""
import argparse
import os
import time
from datetime import datetime

from pyspark.sql import functions as F

from common import OUTPUT_DIR, append_csv, ensure_bucket, get_spark, lake
from transforms import KAFKA_EVENT_SCHEMA, VALID_TYPES, parse_event_ts

JOB = "06_streaming_kpi"
STREAM_DIR = os.path.join(OUTPUT_DIR, "streaming")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="ecom.events")
    ap.add_argument("--kpi-topic", default="ecom.kpi")
    ap.add_argument("--window", default="5 minutes", help="độ dài cửa sổ (thời gian sự kiện)")
    ap.add_argument("--watermark", default="2 minutes", help="chờ dữ liệu trễ tối đa")
    ap.add_argument("--trigger", default="5 seconds")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--max-offsets", type=int, default=300000, help="giới hạn bản ghi / micro-batch")
    ap.add_argument("--run-seconds", type=int, default=0, help="0 = chạy đến khi Ctrl+C")
    ap.add_argument("--reset", action="store_true", help="xóa checkpoint cũ (đọc lại từ đầu)")
    args = ap.parse_args()

    spark = get_spark(JOB)
    ensure_bucket()
    ckpt_events, ckpt_kpi = lake("checkpoints", "stream_events"), lake("checkpoints", "stream_kpi")
    if args.reset:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        for p in (ckpt_events, ckpt_kpi):
            path = jvm.org.apache.hadoop.fs.Path(p)
            path.getFileSystem(conf).delete(path, True)
        print("[stream] đã xóa checkpoint")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_offsets)
           .option("failOnDataLoss", "false")
           .option("fetchOffset.numRetries", 10)
           .option("fetchOffset.retryIntervalMs", 1000)
           .load())

    events = (raw.select(F.from_json(F.col("value").cast("string"), KAFKA_EVENT_SCHEMA).alias("e"),
                         F.col("timestamp").alias("kafka_ts"), "partition", "offset")
              .select("e.*", "kafka_ts", "partition", "offset")
              .withColumn("event_ts", parse_event_ts())
              .withColumn("event_type", F.lower(F.trim("event_type"))))
    valid = events.filter(F.col("event_ts").isNotNull() & F.col("user_id").isNotNull()
                          & F.col("product_id").isNotNull() & F.col("event_type").isin(VALID_TYPES)
                          & F.col("price").isNotNull() & (F.col("price") > 0))

    # ---------------- [A] sự kiện -> Data Lake + đo độ trễ ----------------
    def sink_events(df, batch_id):
        df = df.withColumn("event_date", F.to_date("event_ts")).persist()
        n = df.count()
        if n == 0:
            df.unpersist()
            return
        t0 = time.time()
        (df.drop("kafka_ts").write.mode("append").partitionBy("event_date")
         .parquet(lake("bronze_stream", "events")))
        t_done = time.time() * 1000
        s = df.select((F.lit(t_done) - F.col("produced_at_ms")).alias("lat")).agg(
            F.avg("lat").alias("avg"), F.expr("percentile_approx(lat, 0.5)").alias("p50"),
            F.expr("percentile_approx(lat, 0.95)").alias("p95"),
            F.expr("percentile_approx(lat, 0.99)").alias("p99"), F.max("lat").alias("max")).first()
        append_csv(os.path.join(STREAM_DIR, "latency.csv"), {
            "timestamp": datetime.now().isoformat(timespec="seconds"), "batch_id": batch_id,
            "rows": n, "write_seconds": round(time.time() - t0, 2),
            "lat_avg_ms": round(s["avg"] or 0), "lat_p50_ms": s["p50"], "lat_p95_ms": s["p95"],
            "lat_p99_ms": s["p99"], "lat_max_ms": s["max"]})
        print(f"[A] batch {batch_id}: {n:,} sự kiện -> lake | p95 latency {s['p95']} ms")
        df.unpersist()

    q_events = (valid.writeStream.queryName("events_to_lake")
                .foreachBatch(sink_events)
                .option("checkpointLocation", ckpt_events)
                .trigger(processingTime=args.trigger).start())

    # ---------------- [B] KPI theo cửa sổ -> Kafka ecom.kpi ----------------
    is_type = lambda t: F.when(F.col("event_type") == t, 1).otherwise(0)  # noqa: E731
    kpi = (valid.withWatermark("event_ts", args.watermark)
           .groupBy(F.window("event_ts", args.window))
           .agg(F.sum(is_type("view")).alias("views"),
                F.sum(is_type("cart")).alias("carts"),
                F.sum(is_type("purchase")).alias("purchases"),
                F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)), 2).alias("revenue"),
                F.approx_count_distinct("user_id").alias("active_users"),
                F.max("produced_at_ms").alias("last_produced_at_ms"))
           .select(F.date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
                   F.date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
                   "views", "carts", "purchases", "revenue", "active_users", "last_produced_at_ms",
                   F.round(F.col("purchases") / F.greatest(F.col("views"), F.lit(1)), 5).alias("conversion"),
                   F.round(F.col("carts") / F.greatest(F.col("views"), F.lit(1)), 5).alias("cart_rate")))

    def sink_kpi(df, batch_id):
        df = df.persist()   # dùng 3 lần (collect, Kafka, lake) -> tránh tính lại
        rows = sorted(df.collect(), key=lambda r: r["window_start"])
        if not rows:
            df.unpersist()
            return
        emitted = int(time.time() * 1000)
        out = df.withColumn("emitted_at_ms", F.lit(emitted)).withColumn("batch_id", F.lit(batch_id))
        (out.orderBy("window_start").coalesce(1).select(F.lit("kpi").alias("key"), F.to_json(F.struct(*out.columns)).alias("value"))
         .write.format("kafka").option("kafka.bootstrap.servers", args.bootstrap)
         .option("topic", args.kpi_topic).save())
        out.write.mode("append").parquet(lake("gold", "stream_kpi_windows"))
        for r in rows:
            d = r.asDict()
            d.update(batch_id=batch_id, emitted_at_ms=emitted,
                     window_latency_ms=emitted - (d["last_produced_at_ms"] or emitted))
            append_csv(os.path.join(STREAM_DIR, "kpi_windows.csv"), d)
        print(f"[B] batch {batch_id}: {len(rows)} cửa sổ đã chốt -> {args.kpi_topic} "
              f"(mới nhất {rows[-1]['window_start']}: views={rows[-1]['views']} purchases={rows[-1]['purchases']})")
        df.unpersist()

    q_kpi = (kpi.writeStream.queryName("kpi_windows").outputMode("append")
             .foreachBatch(sink_kpi)
             .option("checkpointLocation", ckpt_kpi)
             .trigger(processingTime=args.trigger).start())

    # ---------------- giám sát tiến độ ----------------
    t_start = time.time()
    try:
        while any(q.isActive for q in (q_events, q_kpi)):
            time.sleep(10)
            for q in (q_events, q_kpi):
                p = q.lastProgress
                if p:
                    append_csv(os.path.join(STREAM_DIR, "progress.csv"), {
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                        "query": p["name"], "batch_id": p["batchId"],
                        "input_rows": p["numInputRows"],
                        "input_rows_per_sec": round(p.get("inputRowsPerSecond") or 0, 1),
                        "processed_rows_per_sec": round(p.get("processedRowsPerSecond") or 0, 1),
                        "trigger_ms": p["durationMs"].get("triggerExecution"),
                        "state_rows": sum(s.get("numRowsTotal", 0) for s in p.get("stateOperators", [])),
                        "watermark": p.get("eventTime", {}).get("watermark", "")})
            if args.run_seconds and time.time() - t_start > args.run_seconds:
                print("[stream] hết thời gian chạy, dừng truy vấn")
                break
    except KeyboardInterrupt:
        print("[stream] dừng theo yêu cầu")
    finally:
        for q in (q_events, q_kpi):
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
