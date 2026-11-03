# ============================================================================
# Lối tắt vận hành ecom-lake. Gõ `make help` để xem danh sách.
# Mặc định dùng dữ liệu MẪU; dữ liệu thật: make ingest RAW="/data/raw/kaggle/*.csv"
# ============================================================================
SPARK  = docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
APPS   = docker compose run --rm producer
RAW   ?= /data/raw/sample/*.csv
ROWS  ?= 500000
K     ?= 4
CORES ?= 4
FORMAT ?= silver
MIN_WEIGHT ?= 3
SPEEDUP ?= 60
LIMIT ?= 0
SKIP ?= 0
ANOMALY ?= none
ANOMALY_START ?= 90
ANOMALY_DURATION ?= 60
STREAM_RAW ?= /data/raw/stream_sample/*.csv

.PHONY: help up down ps sample sample-stream ingest clean gold segment graph pipeline test test-apps validate-pagerank \
        stream stream-reset stream-fresh produce produce-max bench experiments charts size reset logs

help:          ## Danh sách lệnh
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up:            ## Khởi động toàn bộ hệ thống (lần đầu build image ~5-10 phút)
	docker compose up -d --build
	@echo ""
	@echo "  Spark Master UI : http://localhost:8080      History : http://localhost:18080"
	@echo "  MinIO Console   : http://localhost:9001      (minioadmin / minioadmin123)"
	@echo "  Kafka UI        : http://localhost:8090"
	@echo "  API (Swagger)   : http://localhost:8000/docs"
	@echo "  Dashboard       : http://localhost:8501"

down:          ## Dừng hệ thống (giữ dữ liệu)
	docker compose down

ps:            ## Trạng thái các container
	docker compose ps

logs:          ## Log rule engine cảnh báo
	docker compose logs -f alert-engine

# ---------------------------------------------------------------- dữ liệu
sample:        ## Sinh dữ liệu mẫu có lỗi cố ý (ROWS=500000)
	docker compose exec spark-master python3 /opt/scripts/gen_sample.py --rows $(ROWS) --out /data/raw/sample/events_sample.csv

sample-stream: ## Dữ liệu mẫu DÀY cho demo streaming (600k sự kiện / 2 ngày ~ 1000 sự kiện mỗi cửa sổ 5 phút)
	docker compose exec spark-master python3 /opt/scripts/gen_sample.py --rows 600000 --days 2 --seed 7 --out /data/raw/stream_sample/events_stream.csv

# ---------------------------------------------------------------- batch
ingest:        ## CSV -> bronze
	$(SPARK) /opt/jobs/01_ingest_bronze.py --input "$(RAW)"

clean:         ## bronze -> silver + báo cáo chất lượng (task nhỏ hơn để vừa RAM executor 1,4 GB)
	$(SPARK) --conf spark.sql.files.maxPartitionBytes=67108864 --conf spark.sql.shuffle.partitions=200 \
	  /opt/jobs/02_clean_silver.py

gold:          ## silver -> gold KPI (Spark SQL)
	$(SPARK) /opt/jobs/03_kpi_gold.py

segment:       ## RFM + K-Means (K=4)
	$(SPARK) /opt/jobs/04_segment_kmeans.py --k $(K)

graph:         ## Đồ thị sản phẩm mua cùng + PageRank + gợi ý (MIN_WEIGHT=3; dữ liệu mẫu dùng 1)
	$(SPARK) --driver-memory 2g --conf spark.sql.maxPlanStringLength=100000 /opt/jobs/07_product_graph.py --min-weight $(MIN_WEIGHT)

pipeline: ingest clean gold segment graph  ## Chạy toàn bộ batch

# ---------------------------------------------------------------- streaming
stream:        ## Spark Structured Streaming (chạy ở terminal riêng, Ctrl+C để dừng)
	$(SPARK) --conf spark.cores.max=2 --conf spark.ui.port=4041 --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=true /opt/jobs/06_streaming_kpi.py

stream-reset:  ## Như stream nhưng xóa checkpoint, đọc lại topic từ đầu
	$(SPARK) --conf spark.cores.max=2 --conf spark.ui.port=4041 --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=true /opt/jobs/06_streaming_kpi.py --reset --starting-offsets earliest

stream-fresh:  ## Làm sạch phần streaming (cất kết quả cũ, tạo lại topic, xóa bronze_stream/checkpoint, restart rule engine)
	bash scripts/stream_fresh.sh

produce:       ## Phát lại sự kiện vào Kafka (SPEEDUP=60 LIMIT=0 ANOMALY=none|checkout_outage|traffic_drop|traffic_spike)
	$(APPS) python producer.py --input "$(STREAM_RAW)" --speedup $(SPEEDUP) --limit $(LIMIT) --skip $(SKIP) \
	  --anomaly $(ANOMALY) --anomaly-start $(ANOMALY_START) --anomaly-duration $(ANOMALY_DURATION) --tag "$(ANOMALY)"

produce-max:   ## Đo throughput tối đa của ingestion (LIMIT=1000000)
	$(APPS) python producer.py --input "$(STREAM_RAW)" --max-speed --skip $(SKIP) --limit $(if $(filter 0,$(LIMIT)),1000000,$(LIMIT)) --tag max_throughput

# ---------------------------------------------------------------- kiểm thử & thực nghiệm
test:          ## Kiểm thử Spark (pytest, local[2] trong container)
	docker compose exec -w /opt/tests spark-master python3 -m pytest -v

test-apps:     ## Kiểm thử rule engine
	$(APPS) python -m pytest -v test_rules.py

validate-pagerank: ## So PageRank BSP tự cài với networkx
	$(APPS) python validate_pagerank.py

bench:         ## 1 lần benchmark: make bench FORMAT=csv CORES=2
	$(SPARK) --conf spark.cores.max=$(CORES) /opt/jobs/05_benchmark.py --format $(FORMAT)

experiments:   ## Toàn bộ thí nghiệm batch E1–E5 (~1-2 giờ với dữ liệu thật)
	bash scripts/run_experiments.sh

charts:        ## Vẽ biểu đồ + output/charts/summary.md cho báo cáo
	$(APPS) python make_charts.py

size:          ## Dung lượng từng tầng của Data Lake
	docker compose exec spark-master python3 /opt/scripts/lake_admin.py size

reset:         ## Xóa sạch Data Lake (cẩn thận!)
	docker compose exec spark-master python3 /opt/scripts/lake_admin.py reset
