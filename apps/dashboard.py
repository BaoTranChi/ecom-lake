"""
Dashboard nghiệp vụ (Presentation) — Streamlit, đọc các bảng GOLD và số liệu streaming trong output/.
Các tab: Tổng quan · Danh mục & thương hiệu · Khách hàng · Đồ thị sản phẩm · Thời gian thực · Chất lượng dữ liệu
"""
import json
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from segment_names import resolve

OUT = os.environ.get("OUTPUT_DIR", "/app/output")
st.set_page_config(page_title="E-commerce Big Data", page_icon="📊", layout="wide")


def load(rel: str):
    path = os.path.join(OUT, rel)
    return pd.read_csv(path) if os.path.exists(path) else None


def missing(what: str):
    st.info(f"Chưa có dữ liệu — hãy chạy `{what}` trước.")


st.title("📊 Phân tích hành vi khách hàng & doanh thu thương mại điện tử")
st.caption("Kafka → Spark Structured Streaming / Spark batch → Data Lake (MinIO, Parquet bronze/silver/gold) → Dashboard")

tabs = st.tabs(["Tổng quan", "Danh mục & thương hiệu", "Khách hàng (RFM)", "Đồ thị sản phẩm",
                "Thời gian thực", "Chất lượng dữ liệu"])

# ---------------------------------------------------------------- 1. Tổng quan
with tabs[0]:
    daily = load("gold/daily_kpi.csv")
    if daily is None:
        missing("make gold")
    else:
        daily["event_date"] = pd.to_datetime(daily["event_date"])
        dmin, dmax = daily["event_date"].min().date(), daily["event_date"].max().date()
        rng = st.date_input("Khoảng ngày", (dmin, dmax), min_value=dmin, max_value=dmax, key="d")
        d_from, d_to = (rng if isinstance(rng, tuple) and len(rng) == 2 else (dmin, dmax))
        f = daily[(daily["event_date"].dt.date >= d_from) & (daily["event_date"].dt.date <= d_to)]
        c = st.columns(5)
        c[0].metric("Doanh thu", f"${f['revenue'].sum():,.0f}")
        c[1].metric("Đơn mua", f"{f['purchases'].sum():,}")
        c[2].metric("Lượt xem", f"{f['views'].sum():,}")
        c[3].metric("Tỷ lệ xem → mua", f"{f['purchases'].sum() / max(f['views'].sum(), 1):.2%}")
        c[4].metric("Giá trị đơn TB", f"${f['revenue'].sum() / max(f['purchases'].sum(), 1):,.2f}")
        metric = st.radio("Chỉ số", ["revenue", "purchases", "views", "active_users", "view_to_purchase_rate"],
                          horizontal=True)
        st.plotly_chart(px.line(f, x="event_date", y=metric, markers=True, title=f"{metric} theo ngày"),
                        use_container_width=True)
        hourly = load("gold/hourly_activity.csv")
        sess = load("gold/session_funnel.csv")
        l, r = st.columns(2)
        if sess is not None:
            s = sess.iloc[0]
            fn = pd.DataFrame({"Bước": ["Xem", "Thêm giỏ", "Mua"],
                               "Số phiên": [s["sessions_with_view"], s["sessions_with_cart"], s["sessions_with_purchase"]]})
            l.plotly_chart(px.funnel(fn, x="Số phiên", y="Bước", title="Phễu chuyển đổi theo phiên"),
                           use_container_width=True)
        if hourly is not None:
            order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            pv = hourly.pivot_table(index="day_of_week", columns="event_hour", values="events").reindex(order)
            r.plotly_chart(px.imshow(pv, aspect="auto", labels=dict(x="Giờ (UTC)", y="Thứ", color="Sự kiện"),
                                     title="Mật độ hoạt động theo giờ × thứ"), use_container_width=True)

# ---------------------------------------------------------------- 2. Danh mục
with tabs[1]:
    cat, brand, top = load("gold/category_funnel.csv"), load("gold/brand_performance.csv"), load("gold/top_products.csv")
    if cat is None:
        missing("make gold")
    else:
        l, r = st.columns(2)
        l.plotly_chart(px.bar(cat, x="cat_l1", y="revenue", title="Doanh thu theo danh mục cấp 1"),
                       use_container_width=True)
        r.plotly_chart(px.scatter(cat, x="view_to_cart", y="cart_to_purchase", size="revenue", text="cat_l1",
                                  title="Hiệu quả phễu theo danh mục (kích thước = doanh thu)"),
                       use_container_width=True)
        n = st.slider("Số thương hiệu", 5, 50, 15)
        st.plotly_chart(px.bar(brand.head(n), x="brand", y="revenue", color="conversion",
                               title="Top thương hiệu theo doanh thu (màu = tỷ lệ chuyển đổi)"),
                        use_container_width=True)
        st.subheader("Top sản phẩm theo doanh thu")
        st.dataframe(top.head(30), use_container_width=True, hide_index=True)

# ---------------------------------------------------------------- 3. Khách hàng
with tabs[2]:
    prof, ks = load("gold/segment_profile.csv"), load("ml/k_search.csv")
    if prof is None:
        missing("make segment")
    else:
        names = resolve(prof.to_dict("records"), os.path.join(os.environ.get("CONF_DIR", "/app/conf"),
                                                              "segment_actions.json"))
        # ghép số cụm vào tên để biểu đồ không gộp 2 cụm trùng tên
        prof["name"] = prof["segment"].astype(str).map(lambda s: f"Cụm {s} – {names.get(s, {}).get('name', s)}")
        prof["action"] = prof["segment"].astype(str).map(lambda s: names.get(s, {}).get("action", ""))
        l, r = st.columns(2)
        l.plotly_chart(px.pie(prof, names="name", values="customers", title="Tỷ trọng khách hàng"),
                       use_container_width=True)
        r.plotly_chart(px.pie(prof, names="name", values="total_revenue", title="Tỷ trọng doanh thu"),
                       use_container_width=True)
        st.plotly_chart(px.scatter(prof, x="avg_recency_days", y="avg_monetary", size="customers",
                                   color="name", title="Vị trí các phân khúc: Recency × Monetary"),
                        use_container_width=True)
        st.dataframe(prof, use_container_width=True, hide_index=True)
        if ks is not None:
            l, r = st.columns(2)
            l.plotly_chart(px.line(ks, x="k", y="wssse", markers=True, title="Elbow (WSSSE)"), use_container_width=True)
            r.plotly_chart(px.line(ks, x="k", y="silhouette", markers=True, title="Silhouette"), use_container_width=True)

# ---------------------------------------------------------------- 4. Đồ thị
with tabs[3]:
    gpr, comm = load("graph/top_pagerank.csv"), load("graph/communities_top.csv")
    stats_path = os.path.join(OUT, "graph", "graph_stats.json")
    if gpr is None:
        missing("make graph")
    else:
        if os.path.exists(stats_path):
            with open(stats_path) as fh:
                gs = json.load(fh)
            c = st.columns(4)
            c[0].metric("Số đỉnh (sản phẩm)", f"{gs['vertices']:,}")
            c[1].metric("Số cạnh (cặp mua cùng)", f"{gs['edges']:,}")
            c[2].metric("Bậc trung bình", gs["avg_degree"])
            c[3].metric("Thành phần liên thông", f"{gs.get('components', '—')}")
        top20 = gpr.head(20).assign(product=lambda d: d["id"].astype(str))
        fig = px.bar(top20, x="product", y="pagerank", color="cat_l1", hover_data=["brand", "degree"],
                     title="Top 20 sản phẩm theo PageRank")
        fig.update_xaxes(type="category", categoryorder="total descending", title="product_id")
        st.plotly_chart(fig, use_container_width=True)
        deg = load("graph/degree_distribution.csv")
        l, r = st.columns(2)
        if deg is not None:
            l.plotly_chart(px.scatter(deg, x="degree", y="count", log_x=True, log_y=True,
                                      title="Phân phối bậc (log-log)"), use_container_width=True)
        if comm is not None:
            r.dataframe(comm.head(15), use_container_width=True, hide_index=True)
        recs = load("gold/recommendations.csv")
        if recs is not None:
            pid = st.selectbox("Xem gợi ý mua cùng cho sản phẩm", gpr["id"].head(50))
            st.dataframe(recs[recs["product_id"] == pid], use_container_width=True, hide_index=True)


# ---------------------------------------------------------------- 5. Thời gian thực
@st.fragment(run_every=5)
def realtime():
    kw, lat, alerts_path = load("streaming/kpi_windows.csv"), load("streaming/latency.csv"), \
        os.path.join(OUT, "alerts", "alerts.jsonl")
    if kw is None:
        missing("make stream + make produce")
        return
    kw = kw.sort_values("window_start").drop_duplicates("window_start", keep="last")
    kw["window_start"] = pd.to_datetime(kw["window_start"])
    last = kw.iloc[-1]
    al = []
    if os.path.exists(alerts_path):
        with open(alerts_path) as fh:
            al = [json.loads(x) for x in fh.readlines()]
    c = st.columns(5)
    c[0].metric("Cửa sổ mới nhất", str(last["window_start"]))
    c[1].metric("Lượt xem", f"{int(last['views']):,}")
    c[2].metric("Đơn mua", f"{int(last['purchases']):,}")
    c[3].metric("Chuyển đổi", f"{last['conversion']:.2%}")
    if lat is not None and len(lat):
        c[4].metric("Độ trễ p95 (ms)", f"{lat['lat_p95_ms'].iloc[-1]:,.0f}")
    # toàn bộ dòng thời gian + đánh dấu các cửa sổ có cảnh báo (vùng đỏ)
    alert_ws = sorted({pd.to_datetime(a["window_start"]) for a in al if a.get("window_start")})
    figs = [px.line(kw, x="window_start", y=["views", "carts", "purchases"], title="Sự kiện theo cửa sổ 5 phút"),
            px.line(kw, x="window_start", y="conversion", title="Tỷ lệ chuyển đổi theo cửa sổ")]
    for fig in figs:
        for ws in alert_ws:
            fig.add_vrect(x0=ws, x1=ws + pd.Timedelta(minutes=5), fillcolor="red", opacity=0.25, line_width=0)
    if alert_ws:
        figs[1].add_scatter(x=alert_ws, y=[0] * len(alert_ws), mode="markers", name="Cảnh báo",
                            marker=dict(color="red", symbol="triangle-up", size=10))
    l, r = st.columns(2)
    l.plotly_chart(figs[0], use_container_width=True)
    r.plotly_chart(figs[1], use_container_width=True)
    if al:
        st.subheader(f"🚨 Cảnh báo đã kích hoạt ({len(al)})")
        st.dataframe(pd.DataFrame(al[::-1])[["detected_at", "severity", "rule_id", "window_start", "message", "action"]],
                     use_container_width=True, hide_index=True)


with tabs[4]:
    st.caption("Tự làm mới mỗi 5 giây")
    realtime()

# ---------------------------------------------------------------- 6. Chất lượng
with tabs[5]:
    qp = os.path.join(OUT, "quality", "quality_report.json")
    if not os.path.exists(qp):
        missing("make clean")
    else:
        with open(qp) as fh:
            q = json.load(fh)
        c = st.columns(4)
        c[0].metric("Bản ghi bronze", f"{q['total_rows']:,}")
        c[1].metric("Bản ghi silver", f"{q['silver_rows']:,}")
        c[2].metric("Tỷ lệ giữ lại", f"{q['kept_ratio']:.2%}")
        c[3].metric("Bản ghi trùng đã loại", f"{q['r4_duplicates_removed']:,}")
        rules = pd.DataFrame({"Luật": ["R1 thiếu khóa", "R2 event_type sai", "R3 giá ≤ 0", "R4 trùng lặp",
                                       "Thiếu brand (chuẩn hóa)", "Thiếu category (chuẩn hóa)"],
                              "Số bản ghi": [q["r1_missing_key"], q["r2_bad_event_type"], q["r3_bad_price"],
                                             q["r4_duplicates_removed"], q["null_brand"], q["null_category_code"]]})
        st.plotly_chart(px.bar(rules, x="Luật", y="Số bản ghi", log_y=True, title="Vi phạm theo luật (thang log)"),
                        use_container_width=True)
        st.json(q)
