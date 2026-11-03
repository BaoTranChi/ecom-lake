# Kế hoạch đồ án: 4 tuần (tuần 9 đến tuần 12)

Hạn nộp rơi vào tuần 12 (23–29/11/2026); cần hỏi thầy ngày chính xác. Mục tiêu nội bộ: hoàn tất bản nộp trước **thứ Năm 26/11**.
Phạm vi: toàn bộ pipeline, từ Sources → Ingestion → Storage → Processing → Analytics → Presentation đến Decision/Action.

## Tuần 9 (2–8/11): hạ tầng và batch
- [ ] **T2 3/11:** gửi thầy đề cương 1 trang (đề tài, phạm vi, công nghệ, dữ liệu) để xin duyệt
- [ ] Cài Docker Desktop (4 CPU, 10–11 GB RAM), chạy `make up`, `make test`, `make test-apps`
- [ ] Chạy với dữ liệu mẫu: `make sample` rồi `make pipeline MIN_WEIGHT=1`. Mở dashboard và API để kiểm tra
- [ ] Tải `2019-Oct.csv`, chạy `ingest`, `clean`, `gold` trên dữ liệu thật
- [ ] Chụp màn hình: Spark Master (2 worker), Spark UI (DAG/stage của job 02), MinIO (bronze/silver/gold), quality_report
- [ ] Đọc hiểu `transforms.py`, job 01, 02, 03. Tự trả lời các câu hỏi phần A và B trong `VAN_DAP.md`

## Tuần 10 (9–15/11): analytics nâng cao, streaming, viết báo cáo phần đầu
- [ ] `make segment K=4`: xem `k_search.csv`, chốt k, **đặt tên phân khúc** trong `conf/segment_actions.json`
- [ ] `make graph MIN_WEIGHT=3`: xem top PageRank và các cộng đồng
- [ ] Demo streaming với dữ liệu thật, gồm 3 kịch bản bất thường. Chụp Kafka UI, Spark UI tab Structured Streaming, log cảnh báo, tin nhắn Telegram
- [ ] Đọc hiểu job 04, 06, 07 và `rules.py`. Trả lời phần C, D, E trong `VAN_DAP.md`
- [ ] Rà soát bản nháp báo cáo chương 1–5; sửa lại theo cách diễn đạt của mình

## Tuần 11 (16–22/11): thực nghiệm và hoàn thiện báo cáo
- [ ] **Batch** (`make experiments`, khoảng 1–2 giờ):
  - E1: dung lượng lưu trữ
  - E2: CSV so với Parquet
  - E3: chạy với 1, 2, 4 core
  - E4: 7, 14, 31 ngày dữ liệu
  - E5: số shuffle partitions
- [ ] **Streaming:**
  - S1: throughput tối đa của ingestion: `make produce-max STREAM_RAW="/data/raw/kaggle/*.csv" LIMIT=2000000`
  - S2: độ trễ theo tải. Lần lượt chạy `SPEEDUP=60`, `240`, `600`, mỗi mức khoảng 3 phút, với `make stream` đang chạy. So sánh p95 latency và input/processed rate trong `progress.csv`
  - S3: chất lượng phát hiện. Chạy 3 kịch bản `checkout_outage`, `traffic_drop`, `traffic_spike`; ghi lại số cảnh báo đúng, số cảnh báo sai và thời gian phát hiện
  - S4: chịu lỗi. Đang chạy thì Ctrl+C job stream, đợi 1 phút rồi `make stream` lại. Sau đó `make bench FORMAT=bronze_stream`: so `Q1_count` với `Q5_distinct_offsets` và với số bản tin producer đã gửi
- [ ] `make charts`, rồi điền số liệu thật vào chương 6–7 của báo cáo (các chỗ đánh dấu **[ĐIỀN]**)
- [ ] Viết chương 8–9 dựa trên số liệu thật

## Tuần 12 (23–29/11): đóng gói và nộp
- [ ] Phụ lục: hướng dẫn chạy, cấu hình, schema (DATA_CONTRACT), API, link repository, bảng phân công, khai báo dùng AI
- [ ] Quay video demo 5–7 phút theo `VIDEO_DEMO.md`
- [ ] Đẩy lên GitHub (kiểm tra `.gitignore` để không đẩy file CSV lớn), xuất PDF và nộp
- [ ] Ôn toàn bộ `VAN_DAP.md`; tự demo lại một lần từ máy sạch (`make down -v` rồi `make up`)

## Bảng đối chiếu thí nghiệm và tiêu chí chấm
| Bằng chứng | Chứng minh điều gì | Tiêu chí (điểm) |
|---|---|---|
| Pipeline chạy hết từ Sources đến Action, cùng dashboard và API | Hệ thống hoàn chỉnh, kết nối đúng | Hoàn thành (2.5) |
| Kiến trúc medallion, data contract, 14 bài test, code module hóa | Thiết kế và triển khai | Kỹ thuật (2.0) |
| Quality report, log thời gian, ảnh Spark UI/Kafka UI, S3, S4 | Bằng chứng hoạt động | Thực nghiệm (2.0) |
| E3 (số core), E4 (khối lượng), S1, S2 (throughput, latency) | Đặc trưng Big Data, khả năng mở rộng | Big Data & scale (1.5) |
| E2, E5, phân tích phân khúc, PageRank, hạn chế | Phân tích và thảo luận | Phân tích (1.0) |
| README, Makefile, sample generator, phụ lục | Khả năng tái lập | Báo cáo (1.0) |
