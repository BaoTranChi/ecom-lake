"""
Rule engine (Decision/Action) — logic thuần Python, không phụ thuộc Kafka để kiểm thử dễ.

Đầu vào: 1 bản tin KPI của một cửa sổ thời gian (từ job 06), ví dụ
  {"window_start": "...", "window_end": "...", "views": 4321, "carts": 260, "purchases": 71,
   "revenue": 21345.5, "conversion": 0.0164, "active_users": 3012}
Đầu ra: danh sách cảnh báo (dict). Luật cấu hình trong conf/rules.json.

Các kiểu luật:
  zero_with_traffic  metric == 0 trong khi traffic_metric >= min_traffic   (vd: sập thanh toán)
  ratio_low/high     metric / median(lịch sử) < ratio  hoặc  > ratio        (sụt / tăng đột biến)
  zscore_low/high    (metric - mean) / std  < threshold hoặc > threshold     (lệch thống kê)
Cửa sổ bất thường (kể cả khi cảnh báo bị cooldown chặn) KHÔNG được đưa vào lịch sử
-> baseline không bị sự cố kéo dài làm lệch.
"""
import statistics
from collections import deque


class RuleEngine:
    def __init__(self, config: dict):
        self.cfg = config
        self.history_size = config.get("history_size", 30)
        self.min_history = config.get("min_history", 10)
        self.cooldown = config.get("cooldown_windows", 5)
        self.history: dict[str, deque] = {}
        self.last_fired: dict[str, int] = {}
        self.window_index = 0

    def _hist(self, metric: str) -> deque:
        return self.history.setdefault(metric, deque(maxlen=self.history_size))

    def _check(self, rule: dict, kpi: dict):
        """Trả về (fired: bool, observed, baseline, score)."""
        t, m = rule["type"], rule["metric"]
        value = kpi.get(m)
        if value is None:
            return False, None, None, None
        gate = rule.get("traffic_metric", "views")
        if kpi.get(gate, 0) < rule.get("min_traffic", 0):
            return False, value, None, None

        if t == "zero_with_traffic":
            return value == 0, value, None, None

        hist = list(self._hist(m))
        if len(hist) < self.min_history:
            return False, value, None, None
        if t in ("ratio_low", "ratio_high"):
            base = statistics.median(hist)
            if base <= 0:
                return False, value, base, None
            ratio = value / base
            fired = ratio < rule["ratio"] if t == "ratio_low" else ratio > rule["ratio"]
            return fired, value, base, round(ratio, 3)
        if t in ("zscore_low", "zscore_high"):
            mean, std = statistics.fmean(hist), statistics.pstdev(hist)
            if std == 0:
                return False, value, mean, None
            z = (value - mean) / std
            fired = z < rule["threshold"] if t == "zscore_low" else z > rule["threshold"]
            return fired, value, mean, round(z, 2)
        raise ValueError(f"Kiểu luật không hỗ trợ: {t}")

    def evaluate(self, kpi: dict) -> list[dict]:
        self.window_index += 1
        alerts = []
        anomalous = False                      # có luật nào thỏa điều kiện (kể cả bị chặn bởi cooldown)
        for rule in self.cfg["rules"]:
            fired, observed, baseline, score = self._check(rule, kpi)
            if not fired:
                continue
            anomalous = True
            last = self.last_fired.get(rule["id"])
            if last is not None and self.window_index - last <= self.cooldown:
                continue                       # đang trong thời gian chờ -> không spam
            self.last_fired[rule["id"]] = self.window_index
            alerts.append({
                "rule_id": rule["id"],
                "severity": rule.get("severity", "info"),
                "metric": rule["metric"],
                "observed": observed,
                "baseline": round(baseline, 4) if isinstance(baseline, float) else baseline,
                "score": score,
                "window_start": kpi.get("window_start"),
                "window_end": kpi.get("window_end"),
                "action": rule.get("action", "notify"),
                "message": rule.get("message", rule["id"]).format(**{**kpi, "baseline": baseline, "score": score}),
            })
        # chỉ cập nhật lịch sử khi cửa sổ thật sự "bình thường": một cửa sổ bất thường nhưng
        # bị cooldown chặn cảnh báo vẫn KHÔNG được đưa vào baseline (tránh baseline bị kéo lệch)
        if not anomalous:
            for m in {r["metric"] for r in self.cfg["rules"]}:
                if kpi.get(m) is not None:
                    self._hist(m).append(kpi[m])
        return alerts
