# Data Contract — ecom-lake

Tài liệu này ghi giao diện dữ liệu giữa các bước của pipeline: tên, vị trí, định dạng, schema, quy ước và cam kết chất lượng. Mọi thay đổi schema phải cập nhật tài liệu này **trước**, sau đó mới sửa code (`jobs/common.py`, `jobs/transforms.py`).

## 1. Nguồn: CSV REES46 (landing)

| Thuộc tính | Giá trị |
|---|---|
| Nguồn | Kaggle — *eCommerce behavior data from multi category store* (REES46 / Open CDP), tháng 10/2019 |
| Vị trí | `data/raw/kaggle/2019-Oct.csv` (mount vào container: `/data/raw/kaggle/`) |
| Định dạng | CSV, UTF-8, có header, dấu phẩy, sắp xếp tăng dần theo `event_time` |
| Quy mô | ~42,4 triệu dòng, ~5,3 GB (số chính xác lấy từ `output/quality/quality_report.json`) |
| Nguồn thay thế | `scripts/gen_sample.py` sinh dữ liệu cùng schema để tái lập |

| Cột | Kiểu | Bắt buộc | Mô tả | Ví dụ |
|---|---|---|---|---|
| event_time | string (`yyyy-MM-dd HH:mm:ss UTC`) | ✓ | Thời điểm sự kiện (UTC) | `2019-10-01 00:00:00 UTC` |
| event_type | string | ✓ | `view` \| `cart` \| `remove_from_cart` \| `purchase` | `view` |
| product_id | long | ✓ | Mã sản phẩm | `44600062` |
| category_id | long | | Mã danh mục | `2103807459595387724` |
| category_code | string | | Danh mục phân cấp, phân tách bằng `.`; ~30% rỗng | `electronics.smartphone` |
| brand | string | | Thương hiệu (chữ thường); có thể rỗng | `samsung` |
| price | double | ✓ | Giá (USD) | `35.79` |
| user_id | long | ✓ | Mã người dùng (ẩn danh) | `541312140` |
| user_session | string (UUID) | | Mã phiên; đổi khi người dùng quay lại sau thời gian dài | `72d76fde-8bb3-4e00-8c23-a032dfed738c` |

## 2. Kafka

### 2.1 Topic `ecom.events`: Producer → Spark Streaming
| Thuộc tính | Giá trị |
|---|---|
| Partition | 3; **key = user_id**, nên các sự kiện của cùng một người dùng nằm chung một partition và giữ đúng thứ tự |
| Retention | 48 giờ (`KAFKA_LOG_RETENTION_HOURS`), đủ để replay lại khi job streaming lỗi |
| Value | JSON. Gồm 9 trường như mục 1, cộng thêm `produced_at_ms` (epoch ms lúc producer gửi). Trường lỗi được gửi dạng `null` |
| Nén | gzip, `acks=1`, `linger.ms=20` |

```json
{"event_time":"2019-10-01 00:00:04 UTC","event_type":"view","product_id":1004237,"category_id":2053013555631882655,
 "category_code":"electronics.smartphone","brand":"apple","price":1081.98,"user_id":535871217,
 "user_session":"c6bd7419-2748-4c56-95b4-8cec9ff8b80d","produced_at_ms":1793527067944}
```

### 2.2 Topic `ecom.kpi`: Spark Streaming → Alert engine
Mỗi bản tin ứng với **một cửa sổ đã chốt** (output mode `append` + watermark). Key = `window_start`.

| Trường | Kiểu | Mô tả |
|---|---|---|
| window_start, window_end | string `yyyy-MM-dd HH:mm:ss` | Cửa sổ tumbling theo **event time**, mặc định 5 phút |
| views, carts, purchases | long | Số sự kiện theo loại |
| revenue | double | Tổng `price` của các sự kiện purchase |
| active_users | long | Số người dùng phân biệt, **xấp xỉ** (HyperLogLog, sai số khoảng 5%) |
| conversion | double | purchases / views |
| cart_rate | double | carts / views |
| last_produced_at_ms, emitted_at_ms | long | Dùng để đo độ trễ từ lúc sinh sự kiện đến lúc ra quyết định |
| batch_id | long | Micro-batch đã phát ra cửa sổ này |

### 2.3 Topic `ecom.alerts`: Alert engine → hệ thống khác
JSON gồm: `rule_id, severity, metric, observed, baseline, score, window_start, window_end, action, message, detected_at, decision_latency_ms, telegram, webhook`.

## 3. Data Lake: MinIO, bucket `lake`, truy cập qua `s3a://lake/...`

| Tầng | Đường dẫn | Định dạng / phân vùng | Ghi bởi | Chế độ ghi |
|---|---|---|---|---|
| Bronze (batch) | `bronze/events` | Parquet (snappy), `event_date=` | Job 01 | overwrite **động** theo partition |
| Bronze (stream) | `bronze_stream/events` | Parquet, `event_date=` | Job 06 [A] | append theo từng micro-batch |
| Silver | `silver/events` | Parquet, `event_date=` | Job 02 | overwrite |
| Gold | `gold/<bảng>` | Parquet | Job 03, 04, 06 [B], 07 | overwrite (stream: append) |
| Model | `models/rfm_kmeans_k<K>` | Spark ML PipelineModel | Job 04 | overwrite |
| Checkpoint | `checkpoints/stream_*`, `checkpoints/graph` | Nội bộ Spark | Job 06, 07 | — |

### Bronze `bronze/events`
Gồm 9 cột nguồn **giữ nguyên giá trị**, cộng thêm: `event_date` (date, khóa phân vùng), `_source_file` (string), `_ingested_at` (timestamp).

### Silver `silver/events`
| Cột | Kiểu | Quy tắc |
|---|---|---|
| event_ts | timestamp | Parse từ `event_time`; null thì loại (R1) |
| event_date | date | Khóa phân vùng |
| event_hour | int | 0–23 (UTC) |
| day_of_week | string | `Mon`…`Sun` |
| event_type | string | Đã `lower/trim`, ∈ tập hợp lệ (R2) |
| product_id, category_id, user_id | long | product_id và user_id khác null (R1) |
| category_code | string | Rỗng/null thì gán `unknown` |
| cat_l1, cat_l2 | string | Cấp 1 và cấp 2 của category_code; thiếu thì gán `unknown` |
| brand | string | lower/trim; rỗng/null thì gán `unknown` |
| price | double | > 0 (R3) |
| user_session | string | Có thể null |

**Cam kết chất lượng của silver:** không có dòng vi phạm R1–R3, và không có hai dòng trùng trên khóa (`event_ts, event_type, product_id, user_id, user_session`) (R4). Số dòng bị loại theo từng luật được ghi vào `output/quality/quality_report.json` sau mỗi lần chạy.

### Gold
| Bảng | Hạt (grain) | Cột chính |
|---|---|---|
| daily_kpi | 1 dòng / ngày | views, carts, purchases, revenue, active_users, buyers, view_to_purchase_rate, avg_order_value |
| category_funnel | 1 dòng / cat_l1 | views, carts, purchases, revenue, view_to_cart, cart_to_purchase |
| top_products | top 100 sản phẩm | product_id, brand, category_code, purchases, revenue, avg_price |
| brand_performance | top 50 thương hiệu | views, purchases, revenue, conversion |
| hourly_activity | 1 dòng / (thứ, giờ) | events, purchases |
| session_funnel | 1 dòng | sessions_with_view/cart/purchase, total_sessions |
| customer_segments | 1 dòng / khách đã mua | user_id, recency, frequency, monetary, segment |
| product_graph_vertices | 1 dòng / sản phẩm trong đồ thị | id, brand, cat_l1, avg_price, purchases, rank, component, label |
| product_graph_edges | 1 dòng / cặp sản phẩm (src < dst) | src, dst, weight |
| recommendations | ≤ 5 dòng / sản phẩm | product_id, rank, recommended_id, weight, rec_brand, rec_category |
| stream_kpi_windows | 1 dòng / cửa sổ | giống topic `ecom.kpi` |

## 4. Bản xuất phục vụ (`output/`)
Các bảng gold có kích thước nhỏ được collect về driver rồi ghi ra CSV trong `output/gold/`. Dashboard và API đọc trực tiếp từ đây, nên không cần kết nối Spark hay S3. Các số liệu đo đạc nằm ở `output/logs`, `output/benchmarks`, `output/streaming`, `output/alerts`, `output/ml`, `output/graph`.

## 5. Cam kết vận hành (mục tiêu trên máy thí nghiệm 4 core / 16 GB)
| Chỉ tiêu | Mục tiêu | Nơi đo |
|---|---|---|
| Batch silver cho 1 tháng dữ liệu | < 30 phút | `output/logs/runs.csv` |
| Độ trễ p95 từ producer đến Data Lake (stream) | < 15 giây | `output/streaming/latency.csv` |
| Thời gian từ khi cửa sổ đóng đến khi có cảnh báo | < trigger + watermark + 5 giây | `output/alerts/decisions.jsonl` |
| Tỷ lệ giữ lại sau làm sạch | > 99% | `quality_report.json` |
