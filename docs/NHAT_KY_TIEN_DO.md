# Nhật ký tiến độ đồ án

## Ngày 01/11/2026 (Chủ nhật): đã làm được

### Hạ tầng
- [x] Cài Docker Desktop (Mac Intel), cấp 8 CPU / 10 GB RAM / swap 2 GB
- [x] `make up`: 10 container chạy ổn định. Spark 3.5.3 có 1 master + 2 worker (4 core, 6 GB); Kafka 3.8; MinIO; Kafka UI; History Server; API; dashboard; alert engine
- [x] **14 + 1 kiểm thử tự động đạt**: 8 kiểm thử Spark, 7 kiểm thử rule engine (có 1 bài mới tái hiện lỗi baseline)
- [x] Thư mục nộp bài `~/Documents/DoAn_BigData_TranChiBao/` theo mục 7 của đề: 01_BaoCao, 02_MaNguon, 03_DuLieu, 04_MinhChung_Demo, 05_PhanCong
- [x] Script tự chụp minh chứng `scripts/chup_minh_chung.py` (chế độ web / realtime / terminal): khoảng 25 ảnh, không cần chụp tay

### Chạy trên dữ liệu mẫu (500.000 sự kiện): hệ thống chạy đúng từ đầu đến cuối
- [x] Batch: ingest → clean (giữ 99,14%) → 6 bảng KPI → K-Means k=4 (Silhouette 0,5375) → đồ thị (2.414 đỉnh, 6.261 cạnh)
- [x] PageRank tự cài (BSP) khớp GraphFrames: sai lệch tối đa 2,5×10⁻¹⁴, top-100 trùng 100/100
- [x] Streaming: khoảng 1.030 sự kiện/giây; độ trễ p95 ổn định khoảng 8,5 giây (sau khoảng 6 batch khởi động)
- [x] Phát hiện sự cố sập thanh toán ngay ở cửa sổ đầu tiên (05:00); 0/561 cửa sổ sai thứ tự; consumer LAG = 0
- Kết quả dữ liệu mẫu được cất ở `02_MaNguon/ecom-lake/output_dulieumau/`

### Dữ liệu thật Kaggle (2019-Oct.csv, 5,3 GB)
- [x] Tải, giải nén vào `data/raw/kaggle/`
- [x] **Job 01 ingest thành công: 42.448.764 sự kiện trong 255,7 giây**, khớp đúng con số Kaggle
- [ ] Job 02 clean: **lỗi OutOfMemoryError**. Đã có bản sửa **CapNhat_03.zip** (chưa áp dụng, chưa chạy lại)

### Sự cố thật đã gặp và sửa (ghi vào Bảng 5.4 của báo cáo)
| # | Hiện tượng | Nguyên nhân | Khắc phục |
|---|---|---|---|
| 1 | `pull access denied for minio/minio` | MinIO gỡ image cộng đồng khỏi Docker Hub (11/9/2026) | Dùng `cgr.dev/chainguard/minio:latest` + `user: root` |
| 2 | Job 07: `OutOfMemoryError` ở connectedComponents (dữ liệu mẫu) | Thuật toán CC mặc định lặp trên DataFrame, query plan phình to, driver hết RAM khi dựng chuỗi mô tả kế hoạch | `algorithm="graphx"`, `spark.sql.maxPlanStringLength`, `--driver-memory 2g` |
| 3 | Job streaming chết ngay batch đầu: `UnknownTopicOrPartitionException` | Lấy offset qua AdminClient, chỉ thử lại 3 lần mỗi lần cách 10 ms ngay sau khi topic vừa được tạo | `useDeprecatedOffsetFetching=true`, thử lại 10 lần mỗi lần cách 1 giây |
| 4 | Cửa sổ KPI đến rule engine sai thứ tự (259/580) | Key = `window_start` rải bản tin ra 3 partition; Kafka chỉ giữ thứ tự trong 1 partition | Sắp theo thời gian + `coalesce(1)` + key cố định → 0/561 sai thứ tự |
| 5 | REVENUE_SPIKE báo nhầm ngay sau sự cố | Cửa sổ bất thường bị cooldown chặn vẫn được đưa vào baseline; sự cố 60 cửa sổ > lịch sử 36 cửa sổ | Loại mọi cửa sổ bất thường khỏi baseline + kiểm thử tái hiện |
| 6 | Rule engine dừng `FileNotFoundError` | Thư mục output bị dời khi đang chạy | Tạo lại thư mục trước mỗi lần ghi |
| 7 | Job 02 trên 42,4 triệu dòng: 4 executor OOM, treo khoảng 2,5 giờ | `persist` giữ toàn bộ dữ liệu trong RAM (executor 1,4 GB) + cột `_source_file` dài | Bỏ persist, bỏ cột metadata, task 64 MB, 200 partition shuffle |

---

## Ngày mai: việc cần làm (theo thứ tự)

### 0. Khởi động lại (nếu đã tắt máy hoặc tắt Docker)
```bash
open -a Docker            # đợi biểu tượng cá voi đứng yên (Engine running)
cd ~/Documents/DoAn_BigData_TranChiBao/02_MaNguon/ecom-lake
make up && make ps        # cả 10 dịch vụ Up. Dữ liệu bronze vẫn còn trong MinIO
```

### 1. Áp dụng bản sửa và chạy lại batch trên dữ liệu thật (khoảng 30–60 phút)
```bash
cd ~/Documents
unzip -o "$(ls -t ~/Downloads/CapNhat_03*.zip | head -1)"
cd DoAn_BigData_TranChiBao/02_MaNguon/ecom-lake
grep -c "maxPartitionBytes" Makefile      # phải ra 1
caffeinate -i make clean gold segment graph MIN_WEIGHT=3 2>&1 | tee ../../04_MinhChung_Demo/log/pipeline_kaggle_tu02.log
```
- Sau khoảng 10 phút: `grep -c OutOfMemory ../../04_MinhChung_Demo/log/pipeline_kaggle_tu02.log` phải ra **0**. Nếu lớn hơn 0 thì bấm Control + C rồi báo Claude.
- Xong thì gửi kết quả `grep -E "^\[0[0-9]_"` của file log.

### 2. Thực nghiệm batch E1–E5 trên dữ liệu thật (khoảng 1–2 giờ)
```bash
caffeinate -i make experiments 2>&1 | tee ../../04_MinhChung_Demo/log/experiments.log
```

### 3. Streaming chính thức trên dữ liệu thật + thí nghiệm S1–S4
- S1 throughput tối đa, S2 độ trễ theo tải (tua nhanh ×60/×240/×600), S3 ba loại sự cố, S4 chịu lỗi (dừng rồi chạy lại job stream)
- Claude sẽ đưa lệnh cụ thể

### 4. Chụp minh chứng + biểu đồ trên dữ liệu thật
```bash
python scripts/chup_minh_chung.py terminal
python scripts/chup_minh_chung.py
docker compose run --rm producer python make_charts.py
```

### 5. Báo cáo
- Đặt tên/kiểm tra phân khúc (tự động hoặc sửa trong `conf/segment_actions.json`)
- Gửi Claude bản zip tiến độ để điền số liệu thật vào chương 3, 6, 7, 9 (khoảng 80 chỗ [ĐIỀN]) và cập nhật Bảng 5.4 (7 sự cố ở trên)
- Sửa đoạn 6.8.3: PageRank tự cài **nhanh hơn** GraphFrames trên đồ thị nhỏ (ngược với dự đoán ban đầu)

### Còn lại trước hạn nộp (tuần 12, 23–29/11)
- [ ] Gửi thầy Sang đề cương duyệt phạm vi (nếu chưa)
- [ ] Đọc hiểu code + ôn `docs/VAN_DAP.md`
- [ ] Quay video demo theo `docs/VIDEO_DEMO.md`
- [ ] Đẩy lên GitHub, xuất PDF báo cáo, chạy `bash thu_thap_minh_chung.sh --dong-goi`
