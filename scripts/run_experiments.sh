#!/usr/bin/env bash
# Chạy toàn bộ thí nghiệm cho chương "Thực nghiệm và đánh giá".
# Kết quả nối thêm vào output/benchmarks/results.csv và output/benchmarks/storage.csv.
# Dùng sau khi đã chạy ingest + clean trên dữ liệu thật (data/raw/kaggle/*.csv).
set -euo pipefail
cd "$(dirname "$0")/.."

SUBMIT="docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"
BENCH=/opt/jobs/05_benchmark.py
# Lần chạy 1 của mỗi cấu hình là "chạy nguội" (JVM, kết nối S3A) -> biểu đồ bỏ lần 1, lấy trung vị các lần còn lại
REPEAT=${REPEAT:-5}
# Sửa khoảng ngày nếu dùng tháng khác
D0=2019-10-01; W1=2019-10-07; W2=2019-10-14; END=2019-10-31

echo "== E1: Dung lượng lưu trữ CSV vs Parquet =="
docker compose exec -T spark-master python3 /opt/scripts/lake_admin.py size

echo "== E2: Định dạng CSV vs Parquet(bronze) vs Parquet sạch(silver), 4 core =="
for fmt in csv bronze silver; do
  $SUBMIT --conf spark.cores.max=4 $BENCH --format "$fmt" --tag E2_format --repeat $REPEAT
done

echo "== E3: Scale theo số core (silver, cả tháng) =="
for c in 1 2 4; do
  $SUBMIT --conf spark.cores.max=$c $BENCH --format silver --tag E3_cores --repeat $REPEAT
done

echo "== E4: Scale theo khối lượng dữ liệu (silver, 4 core) =="
for to in $W1 $W2 $END; do
  $SUBMIT --conf spark.cores.max=4 $BENCH --format silver --date-from $D0 --date-to $to --tag E4_volume --repeat $REPEAT
done

echo "== E5: Số shuffle partitions (silver, 4 core) =="
for p in 8 64 400; do
  $SUBMIT --conf spark.cores.max=4 --conf spark.sql.shuffle.partitions=$p \
          --conf spark.sql.adaptive.coalescePartitions.enabled=false \
          $BENCH --format silver --tag "E5_partitions_$p" --repeat $REPEAT
done

echo "Xong. Xem output/benchmarks/results.csv"
