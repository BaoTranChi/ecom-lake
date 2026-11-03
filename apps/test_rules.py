"""Kiểm thử rule engine (chạy được không cần Kafka/Spark): pytest apps/test_rules.py"""
import json
import os
import random

from rules import RuleEngine

CONF = os.path.join(os.path.dirname(__file__), "..", "conf", "rules.json")


def engine():
    with open(CONF) as f:
        return RuleEngine(json.load(f))


def normal_kpi(i, rnd):
    views = int(rnd.gauss(4000, 200))
    purchases = int(rnd.gauss(70, 6))
    return {"window_start": f"w{i}", "window_end": f"w{i + 1}", "views": views, "carts": 250,
            "purchases": purchases, "revenue": purchases * rnd.gauss(300, 10),
            "conversion": purchases / views, "active_users": 3000}


def warm(e, n=20, seed=1):
    rnd = random.Random(seed)
    for i in range(n):
        assert e.evaluate(normal_kpi(i, rnd)) == []
    return rnd


def test_no_alert_on_normal_traffic():
    e = engine()
    rnd = warm(e, 20)
    fired = [a for i in range(100) for a in e.evaluate(normal_kpi(100 + i, rnd))]
    assert len(fired) <= 1      # nhiễu ngẫu nhiên gần như không vượt 3-4 sigma


def test_checkout_failure():
    e = engine()
    rnd = warm(e)
    k = normal_kpi(99, rnd)
    k.update(purchases=0, revenue=0.0, conversion=0.0)
    ids = {a["rule_id"] for a in e.evaluate(k)}
    assert "CHECKOUT_FAILURE" in ids and "CONVERSION_DROP" in ids


def test_traffic_drop_and_spike():
    e = engine()
    rnd = warm(e)
    k = normal_kpi(50, rnd)
    k["views"] = 800
    assert any(a["rule_id"] == "TRAFFIC_DROP" for a in e.evaluate(k))
    k = normal_kpi(51, rnd)
    k["views"] = 16000
    assert any(a["rule_id"] == "TRAFFIC_SPIKE" for a in e.evaluate(k))


def test_cooldown_prevents_spam():
    e = engine()
    rnd = warm(e)
    count = 0
    for i in range(6):
        k = normal_kpi(200 + i, rnd)
        k.update(purchases=0, revenue=0.0, conversion=0.0)
        count += sum(a["rule_id"] == "CHECKOUT_FAILURE" for a in e.evaluate(k))
    assert count == 1


def test_baseline_not_polluted_by_anomalies():
    e = engine()
    rnd = warm(e)
    before = list(e.history["views"])
    k = normal_kpi(300, rnd)
    k["views"] = 100
    e.evaluate(k)
    assert list(e.history["views"]) == before


def test_no_decision_before_enough_history():
    e = engine()
    k = {"window_start": "w0", "views": 100, "purchases": 1, "revenue": 1.0, "conversion": 0.01}
    assert e.evaluate(k) == []


def test_long_outage_does_not_poison_revenue_baseline():
    """Sự cố kéo dài 60 cửa sổ (> history_size 36; cooldown chặn bớt cảnh báo) như lần demo thật
    -> khi bán lại bình thường KHÔNG được báo REVENUE_SPIKE do baseline toàn số 0."""
    e = engine()
    rnd = warm(e, 30)
    for i in range(60):
        k = normal_kpi(400 + i, rnd)
        k.update(purchases=0, revenue=0.0, conversion=0.0)
        e.evaluate(k)
    after = [a["rule_id"] for i in range(5) for a in e.evaluate(normal_kpi(500 + i, rnd))]
    assert "REVENUE_SPIKE" not in after
