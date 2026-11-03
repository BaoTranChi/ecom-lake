"""
SOURCES -> INGESTION: phát lại (replay) sự kiện thật từ CSV vào Kafka topic `ecom.events`
như một website đang phát sinh dữ liệu theo thời gian thực.

Chế độ tốc độ:
  --speedup N    giữ đúng nhịp thời gian gốc nhưng tua nhanh N lần (60 = 1 giây thật ~ 1 phút dữ liệu)
  --rate R       cố định R sự kiện/giây
  --max-speed    gửi nhanh nhất có thể (đo throughput tối đa của ingestion)
Chèn bất thường để demo cảnh báo (Decision/Action):
  --anomaly checkout_outage | traffic_drop | traffic_spike  --anomaly-start 60 --anomaly-duration 60

Mỗi bản tin: key = user_id (giữ thứ tự sự kiện của 1 user trong cùng partition),
value = JSON theo docs/DATA_CONTRACT.md, thêm produced_at_ms để đo độ trễ end-to-end.
Số liệu throughput ghi vào output/streaming/producer_metrics.csv.

  python producer.py --input "/data/raw/kaggle/2019-Oct.csv" --speedup 60 --limit 2000000
"""
import argparse
import csv
import glob
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")
INT_FIELDS = ("product_id", "category_id", "user_id")


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="producer-admin")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"[producer] đã tạo topic {topic} ({partitions} partitions)")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def to_message(row: dict) -> dict:
    """Ép kiểu nhẹ; giá trị lỗi để nguyên/None — tầng streaming sẽ lọc (Veracity)."""
    msg = {k: (v if v != "" else None) for k, v in row.items()}
    for k in INT_FIELDS:
        try:
            msg[k] = int(msg[k]) if msg[k] is not None else None
        except ValueError:
            msg[k] = None
    try:
        msg["price"] = float(msg["price"]) if msg["price"] is not None else None
    except ValueError:
        msg["price"] = None
    return msg


def parse_event_time(s: str):
    try:
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


def anomaly_filter(kind: str, msg: dict, rnd: random.Random) -> list:
    """Trả về danh sách bản tin cần gửi cho 1 sự kiện khi bất thường đang diễn ra."""
    if kind == "checkout_outage":          # cổng thanh toán sập: không có purchase nào
        return [] if msg["event_type"] == "purchase" else [msg]
    if kind == "traffic_drop":             # mất 80% lưu lượng (sự cố CDN/DNS)
        return [msg] if rnd.random() < 0.2 else []
    if kind == "traffic_spike":            # bot cào dữ liệu: view tăng 4 lần
        return [msg] * 4 if msg["event_type"] == "view" else [msg]
    return [msg]


def iter_rows(pattern: str):
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"Không tìm thấy file nào khớp {pattern}")
    for path in files:
        with open(path, newline="") as f:
            yield from csv.DictReader(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/data/raw/sample/*.csv")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="ecom.events")
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--speedup", type=float, default=60.0)
    ap.add_argument("--rate", type=float, default=0.0)
    ap.add_argument("--max-speed", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="0 = không giới hạn")
    ap.add_argument("--skip", type=int, default=0, help="bỏ qua N dòng đầu (bắt đầu giữa tháng)")
    ap.add_argument("--anomaly", choices=["none", "checkout_outage", "traffic_drop", "traffic_spike"], default="none")
    ap.add_argument("--anomaly-start", type=float, default=60.0, help="giây (thời gian thật) kể từ lúc bắt đầu")
    ap.add_argument("--anomaly-duration", type=float, default=60.0)
    ap.add_argument("--metrics-every", type=float, default=5.0)
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    ensure_topic(args.bootstrap, args.topic, args.partitions)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
        acks=1, linger_ms=20, batch_size=256 * 1024, compression_type="gzip",
    )

    metrics_path = os.path.join(OUTPUT_DIR, "streaming", "producer_metrics.csv")
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    new_file = not os.path.exists(metrics_path)
    mf = open(metrics_path, "a", newline="")
    mw = csv.writer(mf)
    if new_file:
        mw.writerow(["timestamp", "tag", "mode", "elapsed_s", "sent_total", "rate_eps",
                     "event_time_cursor", "anomaly_active", "skipped_by_anomaly"])
    mode = "max" if args.max_speed else (f"rate{args.rate:g}" if args.rate else f"speedup{args.speedup:g}")

    rnd = random.Random(7)
    sent = skipped = 0
    last_sent, t_start = 0, time.time()
    last_report = t_start
    t0_event = None
    cursor = ""
    try:
        for i, row in enumerate(iter_rows(args.input)):
            if i < args.skip:
                continue
            if i == args.skip and args.skip:
                t_start = last_report = time.time()     # đồng hồ (và mốc sự cố) tính từ dòng đầu tiên được gửi
            if args.limit and sent >= args.limit:
                break
            msg = to_message(row)
            now = time.time()

            # ---- điều tốc ----
            if not args.max_speed:
                if args.rate:
                    target = t_start + sent / args.rate
                else:
                    et = parse_event_time(msg.get("event_time") or "")
                    if et is not None:
                        t0_event = t0_event or et
                        target = t_start + (et - t0_event) / args.speedup
                    else:
                        target = now
                if target - now > 0.005:
                    time.sleep(target - now)
                    now = time.time()

            elapsed = now - t_start
            active = (args.anomaly != "none"
                      and args.anomaly_start <= elapsed < args.anomaly_start + args.anomaly_duration)
            out = anomaly_filter(args.anomaly, msg, rnd) if active else [msg]
            skipped += 1 - min(len(out), 1)
            for m in out:
                m = dict(m, produced_at_ms=int(time.time() * 1000))
                producer.send(args.topic, key=m.get("user_id"), value=m)
                sent += 1
            cursor = msg.get("event_time") or cursor

            if now - last_report >= args.metrics_every:
                rate = (sent - last_sent) / (now - last_report)
                mw.writerow([datetime.now().isoformat(timespec="seconds"), args.tag, mode,
                             round(elapsed, 1), sent, round(rate, 1), cursor, int(active), skipped])
                mf.flush()
                print(f"[producer] t={elapsed:7.1f}s sent={sent:,} rate={rate:,.0f} ev/s "
                      f"event_time={cursor} {'ANOMALY:' + args.anomaly if active else ''}")
                last_sent, last_report = sent, now
    except KeyboardInterrupt:
        print("[producer] dừng theo yêu cầu")
    finally:
        producer.flush()
        producer.close()
        mf.close()
        total = time.time() - t_start
        summary = {"tag": args.tag, "mode": mode, "sent": sent, "seconds": round(total, 2),
                   "avg_rate_eps": round(sent / total, 1) if total else None,
                   "anomaly": args.anomaly, "skipped_by_anomaly": skipped,
                   "finished_at": datetime.now().isoformat(timespec="seconds")}
        with open(os.path.join(OUTPUT_DIR, "streaming", "producer_runs.jsonl"), "a") as f:
            f.write(json.dumps(summary) + "\n")
        print(f"[producer] XONG: {summary}")


if __name__ == "__main__":
    main()
