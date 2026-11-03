"""
API phục vụ kết quả & ra quyết định (Presentation -> Decision/Action), FastAPI.
Tài liệu tương tác tự sinh: http://localhost:8000/docs

Nguồn dữ liệu: các bảng GOLD đã xuất ra output/ (CSV nhỏ, đọc bằng pandas, tự nạp lại khi file đổi).
"""
import json
import os

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from segment_names import resolve

OUT = os.environ.get("OUTPUT_DIR", "/app/output")
CONF = os.environ.get("CONF_DIR", "/app/conf")
app = FastAPI(title="E-commerce Big Data API",
              description="KPI batch, KPI thời gian thực, phân khúc khách hàng, gợi ý sản phẩm và cảnh báo.",
              version="1.0")
_cache: dict = {}


def _csv(rel: str) -> pd.DataFrame:
    path = os.path.join(OUT, rel)
    if not os.path.exists(path):
        raise HTTPException(404, f"Chưa có dữ liệu {rel} — hãy chạy job tương ứng trước")
    mtime = os.path.getmtime(path)
    if rel not in _cache or _cache[rel][0] != mtime:
        _cache[rel] = (mtime, pd.read_csv(path))
    return _cache[rel][1]


def _records(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records", force_ascii=False))


def _jsonl(rel: str, limit: int) -> list[dict]:
    path = os.path.join(OUT, rel)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        lines = f.readlines()[-limit:]
    return [json.loads(x) for x in reversed(lines)]


@app.get("/health")
def health():
    files = ["gold/daily_kpi.csv", "gold/customer_segments.csv", "gold/recommendations.csv",
             "streaming/kpi_windows.csv", "alerts/alerts.jsonl"]
    return {"status": "ok", "data": {f: os.path.exists(os.path.join(OUT, f)) for f in files}}


@app.get("/kpi/daily", summary="KPI theo ngày (batch)")
def kpi_daily(date_from: str | None = None, date_to: str | None = None):
    df = _csv("gold/daily_kpi.csv")
    if date_from:
        df = df[df["event_date"] >= date_from]
    if date_to:
        df = df[df["event_date"] <= date_to]
    total = {"revenue": round(float(df["revenue"].sum()), 2), "purchases": int(df["purchases"].sum()),
             "views": int(df["views"].sum())}
    total["view_to_purchase_rate"] = round(total["purchases"] / max(total["views"], 1), 5)
    return {"summary": total, "days": _records(df)}


@app.get("/kpi/categories", summary="Phễu chuyển đổi theo danh mục")
def kpi_categories(limit: int = 20):
    return _records(_csv("gold/category_funnel.csv").head(limit))


@app.get("/kpi/realtime", summary="KPI các cửa sổ thời gian gần nhất (streaming)")
def kpi_realtime(limit: int = Query(12, le=500)):
    df = _csv("streaming/kpi_windows.csv").sort_values("window_start").tail(limit)
    return _records(df)


@app.get("/alerts", summary="Cảnh báo đã kích hoạt gần nhất")
def alerts(limit: int = Query(20, le=500), rule_id: str | None = None):
    rows = _jsonl("alerts/alerts.jsonl", 100000)
    if rule_id:
        rows = [a for a in rows if a.get("rule_id") == rule_id]
    return rows[:limit]


@app.get("/segments", summary="Hồ sơ các phân khúc khách hàng + tên và hành động gắn kèm")
def segments():
    prof = _csv("gold/segment_profile.csv")
    names = _segment_actions()
    prof = prof.assign(
        name=prof["segment"].astype(str).map(lambda s: names.get(s, {}).get("name")),
        action=prof["segment"].astype(str).map(lambda s: names.get(s, {}).get("action")),
        name_source=prof["segment"].astype(str).map(lambda s: names.get(s, {}).get("source")))
    return _records(prof)


def _segment_actions() -> dict:
    """Tên + hành động cho từng cụm: tự động theo luật, hoặc theo conf nếu người dùng đã đặt."""
    prof = _csv("gold/segment_profile.csv")
    return resolve(prof.to_dict("records"), os.path.join(CONF, "segment_actions.json"))


@app.get("/customers/{user_id}/next-action", summary="Ra quyết định: hành động marketing cho 1 khách")
def next_action(user_id: int):
    df = _csv("gold/customer_segments.csv")
    row = df[df["user_id"] == user_id]
    if row.empty:
        return {"user_id": user_id, "segment": None,
                "action": "Khách chưa từng mua: gợi ý sản phẩm phổ biến (top PageRank)",
                "suggested_products": _records(_csv("graph/top_pagerank.csv").head(5)[["id", "brand", "category_code"]])}
    r = row.iloc[0]
    seg = str(int(r["segment"]))
    m = _segment_actions().get(seg, {})
    return {"user_id": user_id, "segment": int(seg), "segment_name": m.get("name"),
            "rfm": {"recency_days": int(r["recency"]), "frequency": int(r["frequency"]),
                    "monetary": float(r["monetary"])},
            "action": m.get("action"), "channel": m.get("channel")}


@app.get("/products/{product_id}/recommendations", summary="Sản phẩm thường được mua cùng (từ đồ thị)")
def recommendations(product_id: int, k: int = Query(5, le=5)):
    df = _csv("gold/recommendations.csv")
    rec = df[df["product_id"] == product_id].sort_values("rank").head(k)
    if rec.empty:
        raise HTTPException(404, "Sản phẩm không có trong đồ thị mua cùng")
    return {"product_id": product_id, "recommendations": _records(rec)}


@app.get("/graph/top-products", summary="Sản phẩm trung tâm theo PageRank")
def graph_top(limit: int = Query(20, le=100)):
    return _records(_csv("graph/top_pagerank.csv").head(limit))


@app.get("/quality", summary="Báo cáo chất lượng dữ liệu tầng silver")
def quality():
    path = os.path.join(OUT, "quality", "quality_report.json")
    if not os.path.exists(path):
        raise HTTPException(404, "Chưa chạy job 02")
    with open(path) as f:
        return json.load(f)
