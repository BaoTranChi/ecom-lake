"""
PRESENTATION -> DECISION/ACTION: dịch vụ tiêu thụ KPI thời gian thực từ Kafka topic `ecom.kpi`,
áp luật trong conf/rules.json và KÍCH HOẠT HÀNH ĐỘNG:
  1. ghi output/alerts/alerts.jsonl (luôn bật — làm bằng chứng)
  2. đẩy cảnh báo vào Kafka topic `ecom.alerts` (cho hệ thống khác dùng tiếp)
  3. gửi Telegram nếu có TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
  4. POST webhook nếu có ALERT_WEBHOOK_URL
Đo thêm "decision latency" = thời điểm ra quyết định - thời điểm Spark phát KPI.
"""
import json
import os
import time
from datetime import datetime

import requests
from kafka import KafkaConsumer, KafkaProducer

from rules import RuleEngine

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
RULES_PATH = os.environ.get("RULES_PATH", "/app/conf/rules.json")
ICON = {"critical": "🚨", "high": "⚠️", "medium": "🔶", "info": "ℹ️"}


def send_telegram(alert: dict) -> str:
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (token and chat):
        return "skipped"
    text = f"{ICON.get(alert['severity'], '')} [{alert['severity'].upper()}] {alert['rule_id']}\n" \
           f"{alert['message']}\nHành động: {alert['action']}"
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat, "text": text}, timeout=10)
    return f"http_{r.status_code}"


def send_webhook(alert: dict) -> str:
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return "skipped"
    r = requests.post(url, json=alert, timeout=10)
    return f"http_{r.status_code}"


def main():
    with open(RULES_PATH) as f:
        engine = RuleEngine(json.load(f))
    consumer = None
    for attempt in range(30):                      # chờ Kafka sẵn sàng
        try:
            consumer = KafkaConsumer(
                "ecom.kpi", bootstrap_servers=BOOTSTRAP, group_id="alert-engine",
                auto_offset_reset="earliest", enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode()))
            break
        except Exception as e:  # noqa: BLE001
            print(f"[alert] chờ Kafka ({attempt}): {e}")
            time.sleep(5)
    if consumer is None:
        raise SystemExit("Không kết nối được Kafka")
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())

    os.makedirs(os.path.join(OUTPUT_DIR, "alerts"), exist_ok=True)
    alerts_path = os.path.join(OUTPUT_DIR, "alerts", "alerts.jsonl")
    decisions_path = os.path.join(OUTPUT_DIR, "alerts", "decisions.jsonl")
    print(f"[alert] đang lắng nghe ecom.kpi với {len(engine.cfg['rules'])} luật")

    for msg in consumer:
        kpi = msg.value
        t_decide = time.time() * 1000
        alerts = engine.evaluate(kpi)
        latency = round(t_decide - kpi["emitted_at_ms"], 1) if kpi.get("emitted_at_ms") else None
        os.makedirs(os.path.dirname(decisions_path), exist_ok=True)   # thư mục có thể bị dời/xóa khi đang chạy
        with open(decisions_path, "a") as f:     # nhật ký mọi quyết định (kể cả 'không làm gì')
            f.write(json.dumps({"window_start": kpi.get("window_start"), "views": kpi.get("views"),
                                "purchases": kpi.get("purchases"), "conversion": kpi.get("conversion"),
                                "alerts": [a["rule_id"] for a in alerts],
                                "decision_latency_ms": latency}, ensure_ascii=False) + "\n")
        for a in alerts:
            a["detected_at"] = datetime.now().isoformat(timespec="seconds")
            a["decision_latency_ms"] = latency
            a["telegram"] = send_telegram(a)
            a["webhook"] = send_webhook(a)
            producer.send("ecom.alerts", a)
            with open(alerts_path, "a") as f:
                f.write(json.dumps(a, ensure_ascii=False) + "\n")
            print(f"[alert] {ICON.get(a['severity'], '')} {a['rule_id']}: {a['message']}")


if __name__ == "__main__":
    main()
