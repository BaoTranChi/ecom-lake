"""Hàm dùng chung cho mọi job: SparkSession, đường dẫn Data Lake, schema, ghi log thời gian chạy."""
import csv
import os
import time
from contextlib import contextmanager
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3a://lake")
RAW_DIR = os.environ.get("RAW_DIR", "/data/raw")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/output")

# Schema của bộ dữ liệu REES46 "eCommerce behavior data from multi category store".
# Khai báo tường minh thay vì inferSchema: không phải quét toàn bộ file 5+ GB chỉ để đoán kiểu.
RAW_SCHEMA = StructType([
    StructField("event_time", StringType(), True),    # "2019-10-01 00:00:00 UTC"
    StructField("event_type", StringType(), True),    # view | cart | remove_from_cart | purchase
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True), # "electronics.smartphone", có thể rỗng
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])


def lake(*parts: str) -> str:
    """lake('silver', 'events') -> s3a://lake/silver/events"""
    return "/".join([LAKE_ROOT.rstrip("/"), *parts])


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_bucket() -> None:
    """Tạo bucket trên MinIO nếu chưa có (S3A không tự tạo bucket)."""
    if not LAKE_ROOT.startswith("s3a://"):
        return
    import boto3
    bucket = LAKE_ROOT[len("s3a://"):].split("/")[0]
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin123"),
    )
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"[lake] created bucket {bucket}")


def cluster_info(spark: SparkSession) -> dict:
    conf = spark.sparkContext.getConf()
    return {
        "cores_max": conf.get("spark.cores.max", "all"),
        "executor_memory": conf.get("spark.executor.memory", "?"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "default_parallelism": spark.sparkContext.defaultParallelism,
    }


def append_csv(path: str, row: dict) -> None:
    """Ghi thêm 1 dòng vào file CSV (tạo header nếu file mới). Chạy trên driver."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new_file:
            w.writeheader()
        w.writerow(row)


def write_rows_csv(path: str, rows, columns) -> None:
    """Ghi kết quả nhỏ (đã collect về driver) ra CSV cục bộ để dashboard/báo cáo dùng."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            w.writerow([r[c] for c in columns])


@contextmanager
def timed(spark: SparkSession, job: str, step: str, extra: dict | None = None):
    """Đo thời gian 1 bước và ghi vào output/logs/runs.csv — dùng làm bằng chứng thực nghiệm."""
    t0 = time.perf_counter()
    info: dict = {}
    yield info  # bước bên trong có thể gán info["rows"] = ...
    secs = time.perf_counter() - t0
    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "job": job,
        "step": step,
        "seconds": round(secs, 2),
        "rows": info.get("rows", ""),
        **cluster_info(spark),
        **(extra or {}),
    }
    append_csv(os.path.join(OUTPUT_DIR, "logs", "runs.csv"), row)
    print(f"[{job}] {step}: {secs:.2f}s rows={info.get('rows', '-')}")
