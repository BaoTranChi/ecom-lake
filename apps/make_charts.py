"""
Vẽ toàn bộ biểu đồ cho chương Thực nghiệm từ các file số liệu trong output/,
và xuất output/charts/summary.md (bảng số liệu đã tổng hợp để dán vào báo cáo).

  make charts      (chạy trong container apps)
Biểu đồ nào thiếu dữ liệu nguồn sẽ được bỏ qua kèm thông báo.
"""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

OUT = os.environ.get("OUTPUT_DIR", "/app/output")
CH = os.path.join(OUT, "charts")
os.makedirs(CH, exist_ok=True)
plt.rcParams.update({"figure.dpi": 150, "font.size": 10, "axes.grid": True, "grid.alpha": 0.3,
                     "axes.spines.top": False, "axes.spines.right": False})
md: list[str] = ["# Tổng hợp số liệu thực nghiệm\n"]


def read(rel):
    p = os.path.join(OUT, rel)
    return pd.read_csv(p) if os.path.exists(p) else None


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(CH, name))
    plt.close(fig)
    print(f"  ✓ {name}")


def table(df: pd.DataFrame, title: str):
    md.append(f"\n## {title}\n")
    md.append(df.to_markdown(index=False) if hasattr(df, "to_markdown") else df.to_string(index=False))
    md.append("\n")


def bench():
    r = read("benchmarks/results.csv")
    if r is None:
        print("  - bỏ qua benchmark (chưa chạy make experiments)")
        return
    # Lần 1 của mỗi cấu hình là chạy nguội (khởi động JVM, mở kết nối S3A tới MinIO): luôn chậm hơn
    # 2-8 giây bất kể cấu hình -> loại khỏi thống kê, lấy trung vị các lần chạy nóng còn lại.
    if (r["run"] > 1).any():
        r = r[r["run"] > 1]
    keys = ["tag", "format", "date_from", "date_to", "cores_max", "shuffle_partitions", "query"]
    med = r.groupby(keys, as_index=False)["seconds"].median()
    spread = r.groupby(keys)["seconds"].agg(["min", "max", "std", "count"]).reset_index()
    table(spread.round(2), "Độ dao động các lần chạy nóng (giây)")

    e2 = med[med.tag == "E2_format"]
    if len(e2):
        pv = e2.pivot_table(index="query", columns="format", values="seconds")[
            [c for c in ["csv", "bronze", "silver"] if c in set(e2.format)]]
        ax = pv.plot.bar(figsize=(7, 3.8), rot=0)
        ax.set_ylabel("Thời gian (giây, trung vị các lần chạy nóng)")
        ax.set_title("E2 — CSV thô vs Parquet bronze vs Parquet silver (4 core)")
        save(ax.figure, "e2_format.png")
        if "csv" in pv and "silver" in pv:
            pv["speedup_silver_vs_csv"] = (pv["csv"] / pv["silver"]).round(2)
        table(pv.round(2).reset_index(), "E2 — Định dạng lưu trữ (giây)")

    e3 = med[med.tag == "E3_cores"].copy()
    if len(e3):
        e3["cores"] = e3["cores_max"].astype(int)
        pv = e3.pivot_table(index="cores", columns="query", values="seconds").sort_index()
        sp = pv.iloc[0] / pv
        fig, axs = plt.subplots(1, 2, figsize=(10, 3.8))
        pv.plot(marker="o", ax=axs[0])
        axs[0].set_xlabel("Số core (executor)")
        axs[0].set_ylabel("Thời gian (giây)")
        axs[0].set_title("E3 — Thời gian theo số core")
        sp.plot(marker="o", ax=axs[1])
        axs[1].plot(pv.index, pv.index / pv.index[0], "k--", label="Lý tưởng (tuyến tính)")
        axs[1].set_xlabel("Số core")
        axs[1].set_ylabel("Speedup")
        axs[1].set_title("E3 — Speedup so với 1 core")
        axs[1].legend(fontsize=8)
        for a in axs:
            a.set_xticks(list(pv.index))
        save(fig, "e3_scaling_cores.png")
        out = pv.round(2)
        for q in pv.columns:
            out[f"speedup_{q}"] = sp[q].round(2)
            out[f"efficiency_{q}"] = (sp[q] / (pv.index / pv.index[0])).round(2)
        table(out.reset_index(), "E3 — Mở rộng theo số core")

    e4 = med[med.tag == "E4_volume"].copy()
    if len(e4):
        e4["days"] = (pd.to_datetime(e4.date_to) - pd.to_datetime(e4.date_from)).dt.days + 1
        pv = e4.pivot_table(index="days", columns="query", values="seconds").sort_index()
        ax = pv.plot(marker="o", figsize=(7, 3.8))
        ax.set_xlabel("Số ngày dữ liệu")
        ax.set_ylabel("Thời gian (giây)")
        ax.set_xticks(list(pv.index))
        ax.set_title("E4 — Thời gian theo khối lượng dữ liệu (4 core)")
        save(ax.figure, "e4_scaling_volume.png")
        table(pv.round(2).reset_index(), "E4 — Mở rộng theo khối lượng")

    e5 = med[med.tag.str.startswith("E5")].copy()
    if len(e5):
        e5["partitions"] = e5["shuffle_partitions"].astype(int)
        pv = e5.pivot_table(index="partitions", columns="query", values="seconds").sort_index()
        ax = pv.plot.bar(figsize=(7, 3.8), rot=0)
        ax.set_xlabel("spark.sql.shuffle.partitions")
        ax.set_ylabel("Thời gian (giây)")
        ax.set_title("E5 — Ảnh hưởng số partition shuffle (AQE coalesce tắt)")
        save(ax.figure, "e5_partitions.png")
        table(pv.round(2).reset_index(), "E5 — Số shuffle partitions")


def storage():
    s = read("benchmarks/storage.csv")
    if s is None:
        return
    s = s[s.timestamp == s.timestamp.max()]
    order = ["raw_csv/kaggle", "bronze/events", "silver/events"]
    keep = s[s.zone.isin(order)].set_index("zone").reindex(order).dropna(subset=["mb"]).iloc[::-1]
    if len(keep):
        ax = keep["mb"].plot.barh(figsize=(7, 3), color=["#2a9d8f", "#e9c46a", "#264653"][-len(keep):][::-1])
        ax.set_ylabel("")
        base = keep["mb"].get("raw_csv/kaggle")
        for i, (z, v) in enumerate(keep["mb"].items()):
            lab = f"{v:,.0f} MB" + (f"  ({v / base:.0%} so với CSV)" if base and z != "raw_csv/kaggle" else "")
            ax.text(v, i, " " + lab, va="center", fontsize=9)
        ax.set_xlim(0, keep["mb"].max() * 1.45)
        ax.set_xlabel("Dung lượng (MB)")
        ax.set_title("E1 — Dung lượng lưu trữ theo tầng")
        save(ax.figure, "e1_storage.png")
    table(s[["zone", "mb", "files"]], "E1 — Dung lượng Data Lake")


def kmeans():
    k = read("ml/k_search.csv")
    if k is None:
        return
    fig, ax1 = plt.subplots(figsize=(7, 3.8))
    ax1.plot(k.k, k.wssse, "o-", color="tab:blue", label="WSSSE")
    ax1.set_xlabel("k")
    ax1.set_ylabel("WSSSE (elbow)", color="tab:blue")
    ax2 = ax1.twinx()
    ax2.plot(k.k, k.silhouette, "s--", color="tab:orange", label="Silhouette")
    ax2.set_ylabel("Silhouette", color="tab:orange")
    ax1.set_title("Chọn số cụm k cho K-Means (RFM)")
    save(fig, "kmeans_k_search.png")
    table(k, "K-Means — dò k")
    p = read("gold/segment_profile.csv")
    if p is not None:
        table(p, "Hồ sơ phân khúc")


def streaming():
    pm = read("streaming/producer_metrics.csv")
    if pm is not None and len(pm):
        # tách các lần chạy: elapsed_s giảm = bắt đầu lần chạy mới -> chỉ vẽ lần gần nhất
        run_id = (pm.elapsed_s.diff() < 0).cumsum()
        d = pm[run_id == run_id.iloc[-1]]
        fig, ax = plt.subplots(figsize=(8, 3.5))
        ax.plot(d.elapsed_s, d.rate_eps, "-", lw=1.2)
        if d.anomaly_active.any():
            a = d[d.anomaly_active == 1]
            ax.axvspan(a.elapsed_s.min(), a.elapsed_s.max(), color="red", alpha=0.12, label="Bất thường được chèn")
            ax.legend()
        ax.set_xlabel("Thời gian chạy (giây)")
        ax.set_ylabel("Sự kiện / giây")
        ax.set_title("Throughput của producer vào Kafka")
        save(fig, "stream_producer_throughput.png")
    runs = os.path.join(OUT, "streaming", "producer_runs.jsonl")
    if os.path.exists(runs):
        with open(runs) as f:
            table(pd.DataFrame([json.loads(x) for x in f]), "Các lần chạy producer")

    lat = read("streaming/latency.csv")
    if lat is not None and len(lat):
        fig, ax = plt.subplots(figsize=(8, 3.5))
        for c in ["lat_p50_ms", "lat_p95_ms", "lat_p99_ms"]:
            ax.plot(lat.batch_id, lat[c] / 1000, label=c.replace("lat_", "").replace("_ms", ""))
        ax.set_xlabel("Micro-batch")
        ax.set_ylabel("Độ trễ (giây)")
        ax.set_title("Độ trễ end-to-end: producer → Kafka → Spark → Data Lake")
        ax.legend()
        # giai đoạn khởi động: các batch đầu còn xử lý dữ liệu tồn đọng (p95 > 1,5 lần trung vị)
        thr = lat["lat_p95_ms"].median() * 1.5
        warm = next((i for i, v in enumerate(lat["lat_p95_ms"]) if v <= thr), 0)
        ax.axvspan(-0.5, warm - 0.5, color="grey", alpha=0.15)
        ax.text(0, ax.get_ylim()[1] * 0.92, " khởi động", fontsize=8, color="grey")
        save(fig, "stream_latency.png")
        cols = ["rows", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "write_seconds"]
        table(lat[cols].iloc[warm:].describe().round(1).reset_index(), "Độ trễ streaming — giai đoạn ổn định")
        table(lat[cols].iloc[:warm].round(1), f"Độ trễ streaming — {warm} batch khởi động (backlog)")

    pr = read("streaming/progress.csv")
    if pr is not None and len(pr):
        d = pr[(pr["query"] == "events_to_lake") & (pr["input_rows"] > 0)]   # bỏ lúc chạy không tải
        fig, ax = plt.subplots(figsize=(8, 3.5))
        ax.plot(range(len(d)), d.input_rows_per_sec, label="Input rows/s")
        ax.plot(range(len(d)), d.processed_rows_per_sec, label="Processed rows/s")
        ax.set_xlabel("Lần lấy mẫu (10 giây/lần)")
        ax.set_ylabel("Bản ghi / giây")
        ax.set_title("Spark Structured Streaming — tốc độ vào vs tốc độ xử lý")
        ax.legend()
        save(fig, "stream_progress.png")

    kw = read("streaming/kpi_windows.csv")
    if kw is not None and len(kw):
        kw = kw.sort_values("window_start")
        fig, axs = plt.subplots(2, 1, figsize=(9, 5), sharex=True)
        x = pd.to_datetime(kw.window_start)
        axs[0].plot(x, kw.views, label="views")
        axs[0].plot(x, kw.purchases * 20, label="purchases ×20")
        axs[0].legend()
        axs[0].set_title("KPI theo cửa sổ thời gian (streaming)")
        axs[1].plot(x, kw.conversion * 100, color="tab:green")
        axs[1].set_ylabel("Chuyển đổi (%)")
        ap = os.path.join(OUT, "alerts", "alerts.jsonl")
        if os.path.exists(ap):
            with open(ap) as f:
                al = pd.DataFrame([json.loads(x) for x in f])
            for ws in pd.to_datetime(al.window_start.dropna().unique()):
                for a in axs:
                    a.axvline(ws, color="red", alpha=0.35, lw=1)
            table(al.groupby(["rule_id", "severity"]).size().reset_index(name="count"), "Cảnh báo đã kích hoạt")
        save(fig, "stream_kpi_alerts.png")


def quality():
    p = os.path.join(OUT, "quality", "quality_report.json")
    if not os.path.exists(p):
        return
    with open(p) as f:
        q = json.load(f)
    items = {"R1 thiếu khóa": q["r1_missing_key"], "R2 event_type sai": q["r2_bad_event_type"],
             "R3 giá ≤ 0": q["r3_bad_price"], "R4 trùng lặp": q["r4_duplicates_removed"],
             "brand null": q["null_brand"], "category null": q["null_category_code"]}
    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.barh(list(items), [max(v or 0, 0.5) for v in items.values()])
    ax.set_xscale("log")
    ax.set_xlabel("Số bản ghi (thang log)")
    ax.set_title(f"Chất lượng dữ liệu: {q['total_rows']:,} → {q['silver_rows']:,} bản ghi")
    save(fig, "quality.png")
    table(pd.DataFrame([q]).T.reset_index().rename(columns={"index": "chỉ số", 0: "giá trị"}), "Chất lượng dữ liệu")


def graph():
    d = read("graph/degree_distribution.csv")
    if d is not None and len(d):
        fig, ax = plt.subplots(figsize=(6, 3.8))
        ax.loglog(d.degree, d["count"], "o", ms=3)
        ax.set_xlabel("Bậc (số sản phẩm mua cùng)")
        ax.set_ylabel("Số sản phẩm")
        ax.set_title("Phân phối bậc đồ thị sản phẩm (log-log)")
        save(fig, "graph_degree.png")
    p = os.path.join(OUT, "graph", "graph_stats.json")
    if os.path.exists(p):
        with open(p) as f:
            table(pd.DataFrame([json.load(f)]).T.reset_index().rename(columns={"index": "chỉ số", 0: "giá trị"}),
                  "Thống kê đồ thị")
    t = read("graph/top_pagerank.csv")
    if t is not None:
        table(t.head(10), "Top 10 PageRank")


def runs():
    r = read("logs/runs.csv")
    if r is not None:
        last = r.groupby(["job", "step"], as_index=False).last()[["job", "step", "seconds", "rows", "cores_max"]]
        table(last, "Thời gian từng bước (lần chạy gần nhất)")


if __name__ == "__main__":
    print("Vẽ biểu đồ ->", CH)
    for fn in (quality, storage, bench, kmeans, streaming, graph, runs):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            print(f"  ! {fn.__name__}: {e}")
    with open(os.path.join(CH, "summary.md"), "w") as f:
        f.write("\n".join(md))
    print("  ✓ summary.md")
