"""
Tiện ích quản trị Data Lake trên MinIO.
  python3 lake_admin.py size     -> dung lượng & số file theo từng tầng (bronze/silver/gold/models)
                                    ghi thêm vào output/benchmarks/storage.csv
  python3 lake_admin.py reset    -> xóa toàn bộ object trong bucket (làm lại từ đầu)
  python3 lake_admin.py clear P1 P2 ...  -> chỉ xóa các object có tiền tố P1, P2 (vd: bronze_stream/)
"""
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime

import boto3

BUCKET = os.environ.get("LAKE_ROOT", "s3a://lake")[len("s3a://"):].split("/")[0]
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/output")
s3 = boto3.resource(
    "s3",
    endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin123"),
)


def size():
    stats = defaultdict(lambda: [0, 0])
    for obj in s3.Bucket(BUCKET).objects.all():
        parts = obj.key.split("/")
        zone = "/".join(parts[:2]) if len(parts) > 2 else parts[0]
        stats[zone][0] += obj.size
        stats[zone][1] += 1
    raw_dir = os.environ.get("RAW_DIR", "/data/raw")
    for sub in ("kaggle", "sample"):
        d = os.path.join(raw_dir, sub)
        if os.path.isdir(d):
            files = [os.path.join(d, x) for x in os.listdir(d) if x.endswith(".csv")]
            stats[f"raw_csv/{sub}"] = [sum(os.path.getsize(x) for x in files), len(files)]

    out = os.path.join(OUTPUT_DIR, "benchmarks", "storage.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    new = not os.path.exists(out)
    with open(out, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["timestamp", "zone", "bytes", "mb", "files"])
        ts = datetime.now().isoformat(timespec="seconds")
        print(f"{'zone':35s} {'MB':>10s} {'files':>7s}")
        for zone, (b, n) in sorted(stats.items()):
            print(f"{zone:35s} {b / 1e6:10.1f} {n:7d}")
            w.writerow([ts, zone, b, round(b / 1e6, 2), n])


def reset():
    s3.Bucket(BUCKET).objects.all().delete()
    print(f"Đã xóa toàn bộ object trong bucket '{BUCKET}'")


def clear(prefixes):
    if not prefixes:
        sys.exit("Cần ít nhất 1 tiền tố, vd: clear bronze_stream/")
    for p in prefixes:
        objs = list(s3.Bucket(BUCKET).objects.filter(Prefix=p))
        for i in range(0, len(objs), 1000):
            s3.Bucket(BUCKET).delete_objects(Delete={"Objects": [{"Key": o.key} for o in objs[i:i + 1000]]})
        print(f"  xóa {len(objs):6d} object  {BUCKET}/{p}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "size"
    if cmd == "clear":
        clear(sys.argv[2:])
    else:
        {"size": size, "reset": reset}.get(cmd, size)()
