#!/usr/bin/env bash
# Đưa phần STREAMING về trạng thái sạch trước một lượt chạy chính thức (KHÔNG đụng bronze/silver/gold).
#   1. Cất kết quả streaming/cảnh báo cũ vào output_luu/<nhãn>/
#   2. Xóa + tạo lại 3 topic Kafka (bỏ sự kiện của các lần chạy trước)
#   3. Xóa bronze_stream và checkpoint streaming trên MinIO
#   4. Khởi động lại rule engine (xóa baseline đang giữ trong RAM)
# Dùng: bash scripts/stream_fresh.sh [nhãn]      (hoặc make stream-fresh)
set -uo pipefail
cd "$(dirname "$0")/.."
TAG=${1:-$(date +%Y%m%d_%H%M)}
KT="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

echo "== 1. Cất kết quả cũ vào output_luu/$TAG"
mkdir -p "output_luu/$TAG"
for d in streaming alerts; do
  if [ -d "output/$d" ]; then mv "output/$d" "output_luu/$TAG/"; echo "   output/$d -> output_luu/$TAG/$d"; fi
done

echo "== 2. Xóa và tạo lại topic Kafka"
docker compose stop alert-engine >/dev/null 2>&1
for t in ecom.events ecom.kpi ecom.alerts; do $KT --delete --topic "$t" >/dev/null 2>&1 || true; done
sleep 5
$KT --create --if-not-exists --topic ecom.events --partitions 3 --replication-factor 1
$KT --create --if-not-exists --topic ecom.kpi    --partitions 1 --replication-factor 1
$KT --create --if-not-exists --topic ecom.alerts --partitions 1 --replication-factor 1
$KT --describe --topic ecom.events | head -1

echo "== 3. Xóa bronze_stream + checkpoint streaming trên MinIO"
docker compose exec -T spark-master python3 /opt/scripts/lake_admin.py clear \
  bronze_stream/ checkpoints/stream_events/ checkpoints/stream_kpi/

echo "== 4. Khởi động lại rule engine"
docker compose start alert-engine
echo "Xong. Tiếp theo: make stream-reset (tab stream) rồi make produce ... (tab producer)"
