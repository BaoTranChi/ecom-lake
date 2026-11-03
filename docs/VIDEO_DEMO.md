# Kịch bản video demo (5–7 phút)

**Chuẩn bị trước khi quay:**
- `make up` đã chạy và batch đã chạy xong trên dữ liệu thật.
- Mở sẵn các tab trình duyệt: Spark Master :8080, MinIO :9001, Kafka UI :8090, Dashboard :8501, API :8000/docs.
- Màn hình chia đôi: một nửa terminal, một nửa trình duyệt. Tăng cỡ chữ terminal.
- Nếu muốn demo Telegram, cấu hình `.env` và mở sẵn Telegram trên điện thoại hoặc web.

| Thời điểm | Màn hình | Lời thoại gợi ý |
|---|---|---|
| 0:00–0:30 | Slide hoặc ảnh kiến trúc | "Em là Trần Chí Bảo. Đồ án xây dựng pipeline dữ liệu lớn đầy đủ cho dữ liệu hành vi thương mại điện tử, khoảng 42 triệu sự kiện mỗi tháng…" |
| 0:30–1:00 | `make ps`, Spark Master :8080 | Giới thiệu 10 container; cluster có 2 worker, 4 core. |
| 1:00–2:00 | Terminal: `make clean` đang chạy; Spark UI :4040 tab Jobs/Stages | Giải thích luật R1–R4. Chỉ ranh giới stage do shuffle khi khử trùng. Kết thúc bằng bảng báo cáo chất lượng. |
| 2:00–2:30 | MinIO Console: `lake/bronze/events/event_date=…`, `silver`, `gold` | Data Lake ba tầng, Parquet phân vùng theo ngày. |
| 2:30–3:30 | Dashboard: tab Tổng quan → Khách hàng → Đồ thị | Đổi bộ lọc ngày. Diễn giải 1–2 insight thật, ví dụ danh mục doanh thu cao nhất, phân khúc VIP chiếm x% doanh thu, sản phẩm trung tâm theo PageRank. |
| 3:30–4:00 | API :8000/docs: thử `/customers/{id}/next-action` và `/products/{id}/recommendations` | Từ insight đến quyết định cho từng khách. |
| 4:00–5:30 | Terminal 1: `make stream`; Terminal 2: `make produce … ANOMALY=checkout_outage`; Kafka UI hiển thị offset tăng; Dashboard tab Thời gian thực | Dữ liệu chảy vào theo thời gian thực. Khi sự cố bắt đầu, số đơn mua về 0, cảnh báo CHECKOUT_FAILURE xuất hiện trên dashboard, trong log và trên Telegram. |
| 5:30–6:15 | `output/charts/e3_scaling_cores.png`, `stream_latency.png` | Tóm tắt số liệu thực nghiệm: speedup, p95 latency. |
| 6:15–6:45 | Ctrl+C job stream rồi chạy lại `make stream` | Chịu lỗi: job đọc tiếp từ checkpoint. |
| 6:45–7:00 | Ảnh kiến trúc | Kết luận và hướng phát triển. |

Mẹo: quay bằng QuickTime (⌘⇧5), chỉ ghi vùng màn hình cần thiết. Những đoạn chờ job chạy lâu thì cắt bớt và ghi chú "(tua nhanh)".
