# Chuẩn bị vấn đáp: câu hỏi thầy có thể hỏi và cách trả lời

Mỗi câu hỏi kèm ý chính và chỗ trong code để chỉ cho thầy. Đừng học thuộc nguyên văn, hãy hiểu rồi nói bằng lời của mình. Những câu đánh ⭐ rất hay bị hỏi.

## A. Bài toán và đặc trưng Big Data
**A1 ⭐. Vì sao bài toán này cần Big Data mà không dùng Pandas?**
- *Volume:* một tháng có khoảng 42 triệu dòng và 5,3 GB CSV. Pandas phải nạp toàn bộ vào RAM, và khi ở dạng DataFrame, dung lượng thường gấp 2–5 lần file CSV, tức 10–25 GB. Con số này vượt RAM 16 GB của máy. Nếu lấy cả 7 tháng của bộ REES46 thì dữ liệu lên hơn 50 GB.
- *Velocity:* sự kiện phát sinh liên tục, và phát hiện sự cố thanh toán phải tính bằng giây, không thể đợi báo cáo cuối ngày.
- *Veracity:* khoảng 30% thiếu category_code, có bản ghi trùng, có giá ≤ 0. Cần luật làm sạch có kiểm soát và có báo cáo.
- *Value:* KPI, phân khúc khách hàng, gợi ý sản phẩm và cảnh báo đều dẫn đến hành động cụ thể.
- Ý chốt theo slide Chương 1: "lớn" là khi kiến trúc phải thay đổi, không phải vượt một ngưỡng GB cố định.

**A2. Dữ liệu chỉ 5 GB, có thật sự là Big Data không?**
Đây là bài toán Big Data thu nhỏ để chạy trên laptop. Kiến trúc được thiết kế để mở rộng ngang: muốn xử lý nhiều hơn thì thêm worker Spark, thêm partition Kafka, thêm node MinIO, không phải viết lại code. Thí nghiệm E3 và E4 đo xu hướng mở rộng. Nêu thẳng hạn chế: một máy vật lý nên I/O và CPU vẫn dùng chung.

**A3. Đồ án phụ trách giai đoạn nào?**
Toàn bộ pipeline. Ranh giới: bắt đầu từ file CSV nguồn, kết thúc ở cảnh báo, hành động và API. Mỗi giai đoạn có đầu vào và đầu ra rõ ràng, ghi trong `docs/DATA_CONTRACT.md`.

## B. Lưu trữ (Chương 4)
**B1 ⭐. Vì sao dùng Parquet thay vì giữ CSV?**
Parquet lưu theo cột, nên truy vấn chỉ đọc những cột cần dùng (column pruning). Dữ liệu được nén (snappy, cộng mã hóa dictionary và RLE cho cột lặp như brand, event_type). File có thống kê min/max để bỏ qua các khối không liên quan. Schema có kiểu được lưu ngay trong file. Thí nghiệm E1 so dung lượng, E2 so tốc độ.

**B2 ⭐. Vì sao phân vùng theo `event_date`?**
Hầu hết truy vấn lọc theo khoảng ngày. Khi phân vùng theo ngày, Spark chỉ đọc đúng thư mục `event_date=...` cần thiết (partition pruning). Số partition vừa phải: 31 thư mục/tháng, mỗi thư mục khoảng 40–60 MB. Không phân vùng theo `user_id` vì sẽ sinh hàng trăm nghìn file nhỏ.

**B3. Bronze, silver, gold là gì? Sao không làm sạch luôn lúc nạp?**
Bronze giữ nguyên dữ liệu gốc. Khi luật làm sạch thay đổi, chỉ cần chạy lại từ bronze, không phải nạp lại nguồn. Silver là dữ liệu sạch, chuẩn hóa, dùng chung cho mọi phân tích. Gold là dữ liệu đã tổng hợp theo nhu cầu nghiệp vụ. Ba tầng tách trách nhiệm và giúp truy vết lỗi về đúng tầng.

**B4. Vì sao MinIO mà không dùng HDFS?**
Cả hai đều là lưu trữ phân tán. MinIO cung cấp S3 API, chuẩn chung của Data Lake trên cloud, nên code chuyển sang AWS S3 chỉ cần đổi endpoint. MinIO cũng nhẹ khi chạy trên laptop. Đánh đổi: không có data locality như HDFS (tính toán không nằm cạnh dữ liệu), rename chậm (S3A commit). Trên cluster thật có thể dùng HDFS hoặc S3; code Spark gần như giữ nguyên.

**B5. Replication và chịu lỗi của lưu trữ trong đồ án?**
Bản demo chạy MinIO một node nên không có replication; đây là hạn chế cần nêu. Môi trường thật dùng MinIO distributed mode với erasure coding, hoặc HDFS với hệ số nhân bản RF=3 có nhận biết rack (rack awareness). Kafka cũng chạy một broker với replication factor 1.

## C. Xử lý với Spark (Chương 2, 3, 5)
**C1 ⭐. Spark khác MapReduce ở điểm nào? Sao chọn Spark?**
Chuỗi job MapReduce phải ghi kết quả trung gian xuống đĩa (HDFS) giữa các job. Spark lập DAG, gộp các narrow transformation vào cùng một stage, và có thể cache dữ liệu dùng lại. Trong đồ án, job 02 dùng `persist` vì tập đã gắn cờ được dùng hai lần. PageRank và K-Means là thuật toán lặp, đúng loại workload MapReduce xử lý kém.

**C2 ⭐. Chỉ ra narrow và wide dependency trong job của cậu.**
- Narrow (không shuffle): `withColumn`, `filter`, `select` trong `add_quality_flags`, `clean_events`.
- Wide (có shuffle): `dropDuplicates` (khử trùng), `groupBy` trong KPI và RFM, `join` khi xây cạnh đồ thị, `repartition("event_date")` trước khi ghi.
Mở Spark UI để chỉ ranh giới stage tại các bước shuffle.

**C3. Driver, executor, cluster manager trong hệ thống của cậu là gì?**
Cluster manager là Spark standalone master. Hai worker, mỗi worker 2 core và 3 GB, cung cấp tối đa 4 executor, mỗi executor 1 core và 1,4 GB. Driver chạy trong container spark-master (client mode). Mỗi action sinh job, job chia thành stage, stage chia thành task; mỗi task xử lý một partition.

**C4. Vì sao đặt `spark.sql.shuffle.partitions = 64` thay vì mặc định 200?**
Với 4 core và vài GB dữ liệu, 200 partition tạo nhiều task rất nhỏ, chi phí lập lịch lớn hơn lượng công việc. AQE còn tự gộp partition nhỏ lại. Thí nghiệm E5 so 8, 64, 400 khi tắt tính năng gộp: quá ít thì mỗi task nặng, dễ tràn đĩa; quá nhiều thì tốn chi phí điều phối.

**C5. Vì sao khai báo schema thay vì `inferSchema`?**
`inferSchema` phải quét thêm một lượt toàn bộ 5 GB chỉ để đoán kiểu. Ngoài ra kiểu có thể bị đoán sai, ví dụ `category_id` dài 19 chữ số. Schema tường minh cũng chính là data contract.

**C6. Khử trùng theo khóa nào? Vì sao?**
Khóa gồm `event_ts, event_type, product_id, user_id, user_session`. Hai bản ghi trùng toàn bộ khóa này là do lỗi ghi log hoặc gửi lại. Không khử trùng chỉ theo `(user_id, product_id)`, vì người dùng có thể xem một sản phẩm nhiều lần thật.

**C7. MapReduce có trong đồ án không?**
Mô hình map → shuffle → reduce nằm trong mọi `groupBy` của Spark. Ví dụ `daily_kpi`: map phát ra (ngày, giá), shuffle theo ngày, reduce lấy tổng. Có thể giải thích job 03 theo tư duy key-value của Chương 3. Thí nghiệm E2 cũng cho thấy chi phí khi phải đọc lại CSV, giống như mỗi job MapReduce đọc lại HDFS.

## D. Streaming (Chương 6)
**D1 ⭐. Event time khác processing time thế nào? Cậu dùng cái nào?**
Event time là lúc sự kiện xảy ra (`event_time` trong dữ liệu). Processing time là lúc Spark xử lý. Đồ án gom cửa sổ theo event time để KPI đúng với thực tế ngay cả khi dữ liệu đến trễ hoặc được replay. Độ trễ thì đo theo đồng hồ thật (`produced_at_ms`).

**D2 ⭐. Watermark để làm gì? Đặt 2 phút có ý nghĩa gì?**
Watermark là ngưỡng thời gian Spark chấp nhận chờ dữ liệu đến muộn: watermark bằng event time lớn nhất đã thấy trừ đi 2 phút. Cửa sổ có thời điểm kết thúc nhỏ hơn watermark thì được chốt, phát ra một lần (chế độ append), và state của nó bị xóa. Nhờ vậy state store không phình mãi. Đánh đổi: watermark càng dài thì càng ít mất sự kiện muộn, nhưng cảnh báo càng chậm.

**D3. Vì sao output mode `append` cho KPI?**
Rule engine cần mỗi cửa sổ đúng một lần, với giá trị cuối cùng. Chế độ `update` sẽ phát nhiều phiên bản của cùng cửa sổ, làm cảnh báo bị trùng hoặc sai.

**D4 ⭐. Nếu job streaming chết thì sao? Có mất hay trùng dữ liệu không?**
Checkpoint trên S3A lưu offset Kafka đã xử lý và state của các cửa sổ. Khi khởi động lại, Spark đọc tiếp từ offset cuối nên không mất dữ liệu. Kafka giữ dữ liệu 48 giờ nên luôn có thể replay. Sink `foreachBatch` là *at-least-once*: nếu job chết sau khi ghi Parquet nhưng trước khi commit batch, batch đó sẽ được ghi lại. Thí nghiệm S4 đo đúng điều này bằng cách so `count` với `distinct(partition, offset)`. Muốn exactly-once thì cần sink có tính idempotent, ví dụ ghi đè theo `batch_id` hoặc dùng Delta Lake. Đây là hướng cải tiến.

**D5. Vì sao key Kafka là `user_id`? Vì sao 3 partition?**
Cùng user thì vào cùng partition, nên thứ tự sự kiện của mỗi user được giữ (Kafka chỉ đảm bảo thứ tự trong một partition). 3 partition cho phép tối đa 3 task Spark đọc song song, phù hợp 2 core dành cho job streaming. Muốn mở rộng thì tăng partition và thêm consumer.

**D6. Lambda hay Kappa?**
Gần với Lambda: có nhánh batch (độ chính xác cao, lịch sử đầy đủ) và nhánh stream (độ trễ thấp). Điểm khác là cả hai cùng dùng một engine Spark và cùng logic lọc, nên giảm được nhược điểm "viết logic hai lần" của Lambda. Có thể bàn thêm hướng Kappa: replay Kafka để tính lại lịch sử.

**D7. Độ trễ đo thế nào?**
Producer gắn `produced_at_ms` vào từng bản tin. Sau khi micro-batch ghi xong Parquet, Spark lấy thời điểm đó trừ `produced_at_ms`, rồi tính p50/p95/p99. Độ trễ này gồm thời gian nằm trong Kafka, chờ trigger, xử lý và ghi xuống lake. Có giả định: đồng hồ của các container dùng chung đồng hồ máy chủ Docker.

## E. Phân tích (Chương 7, 8)
**E1 ⭐. RFM là gì? Vì sao log1p và chuẩn hóa trước K-Means?**
Recency: số ngày từ lần mua cuối. Frequency: số phiên có mua. Monetary: tổng tiền. F và M lệch phải rất mạnh (vài khách chi rất nhiều), nên log1p kéo phân phối gọn lại. K-Means dùng khoảng cách Euclid, nên nếu không chuẩn hóa thì chiều Monetary (hàng nghìn đô) sẽ lấn át chiều Recency (vài chục ngày).

**E2 ⭐. Chọn k thế nào?**
Chạy k từ 2 đến 8 trên mẫu 30%, xem WSSSE (tìm điểm khuỷu tay) và Silhouette (càng gần 1 càng tốt). Kết hợp với khả năng diễn giải nghiệp vụ: k=4 thường cho các nhóm dễ đặt tên và dễ gắn hành động. Ghi rõ lý do chọn theo số liệu thật.

**E3. Hạn chế của RFM trên dữ liệu một tháng?**
Recency chỉ nằm trong khoảng 1–31 ngày và frequency phần lớn bằng 1. Phân khúc vì thế chủ yếu dựa vào Monetary và Recency. Nếu có nhiều tháng dữ liệu thì phân khúc có ý nghĩa hơn.

**E4 ⭐. Đồ thị của cậu là gì? Vì sao cần graph computing?**
Đỉnh là sản phẩm. Cạnh nối hai sản phẩm cùng được thêm giỏ hoặc mua trong một phiên; trọng số là số phiên. Câu hỏi "sản phẩm nào là trung tâm của mạng mua sắm" và "nhóm sản phẩm nào hay đi cùng nhau" là câu hỏi về quan hệ, nên bảng KPI không trả lời được.

**E5 ⭐. PageRank hoạt động thế nào? Kiểu Pregel/BSP là gì?**
Mỗi superstep, mỗi đỉnh gửi `rank/outdeg` cho hàng xóm (message passing). Mỗi đỉnh gom các message lại và tính `rank = 0,15 + 0,85 × tổng message`. Lặp 10 lần. Đồ án tự cài đặt theo kiểu này bằng DataFrame (`pagerank_bsp`: join là gửi message, groupBy là gom message), rồi so với GraphFrames. Bản tự cài đã được kiểm tra khớp với networkx, sai lệch dưới 0,001.

**E6. Vì sao bỏ phiên có quá 30 sản phẩm?**
Một phiên có n sản phẩm sinh ra n(n−1)/2 cặp. Một phiên 500 sản phẩm (thường là bot) sinh hơn 124.000 cạnh nhiễu. Ngưỡng `min_weight` loại các cặp chỉ xuất hiện ngẫu nhiên một lần.

**E7. Connected components và label propagation cho biết gì?**
Connected components cho biết đồ thị có bị tách rời thành nhiều cụm không; thường có một cụm khổng lồ. Label propagation tìm cộng đồng sản phẩm hay mua cùng nhau. Mỗi cộng đồng được mô tả bằng danh mục và thương hiệu chiếm đa số.

## F. Ra quyết định, trình bày, vận hành
**F1 ⭐. Luật cảnh báo hoạt động thế nào? Vì sao không dùng ngưỡng cố định?**
Lưu lượng thay đổi theo giờ trong ngày, nên ngưỡng cố định sẽ báo nhầm vào ban đêm. Rule engine so mỗi cửa sổ với lịch sử gần nhất (36 cửa sổ): dùng tỷ lệ so với trung vị cho lưu lượng, và z-score cho tỷ lệ chuyển đổi và doanh thu. Có ba cơ chế bổ sung:
- *gating:* chỉ đánh giá khi có ít nhất 500 lượt xem;
- *cooldown:* không lặp lại cùng một cảnh báo trong 6 cửa sổ;
- cửa sổ bất thường không được đưa vào lịch sử, để baseline không bị kéo lệch.
Có 6 kiểm thử tự động trong `apps/test_rules.py`.

**F2. Làm sao biết luật tốt?**
Thí nghiệm S3: chèn sự cố có kiểm soát vào producer, rồi đo phát hiện đúng (có bắt được không), báo nhầm (cảnh báo ngoài thời gian sự cố) và thời gian phát hiện.

**F3. "Next-best-action" là gì?**
Endpoint `/customers/{id}/next-action` tra phân khúc RFM của khách, rồi trả về hành động marketing tương ứng trong `conf/segment_actions.json`. Đây là bước biến insight thành quyết định ở mức từng khách.

**F4. Muốn đưa lên production cần thêm gì?**
- MinIO và Kafka nhiều node có replication.
- Spark trên YARN hoặc Kubernetes.
- Bộ lập lịch như Airflow cho các job batch.
- Delta Lake hoặc Iceberg để có ACID và time travel.
- Schema registry cho Kafka.
- Giám sát bằng Prometheus và Grafana.
- Bảo mật: TLS, IAM, không để mật khẩu trong file cấu hình.

**F5. Phần nào cậu dùng AI? Cậu đã kiểm tra gì?**
Trả lời trung thực theo phụ lục: AI hỗ trợ dựng khung code và tài liệu. Cậu đã tự chạy, debug những lỗi gặp phải (kể ra cụ thể), đọc hiểu từng job, tự chọn k và đặt tên phân khúc, tự phân tích số liệu thực nghiệm.
