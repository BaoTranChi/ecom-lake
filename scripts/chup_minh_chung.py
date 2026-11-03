"""
Tự động chụp ảnh minh chứng các giao diện web của hệ thống (trình duyệt chạy ngầm).

Cài 1 lần:   pip install playwright && python -m playwright install chromium
Chạy:        python scripts/chup_minh_chung.py            (đứng ở thư mục ecom-lake)
             python scripts/chup_minh_chung.py realtime   (chỉ chụp phần streaming, khi đang demo)
             python scripts/chup_minh_chung.py terminal   (chạy make ps / make test, lưu log + ảnh kiểu Terminal)

Ảnh lưu vào ../../04_MinhChung_Demo/anh_chup/, phản hồi API lưu dạng JSON vào ../../04_MinhChung_Demo/log/api/.
Chạy lại bao nhiêu lần cũng được (ghi đè ảnh cũ). Trang nào lỗi sẽ bỏ qua và báo ở cuối.
"""
import base64
import csv
import json
import os
import sys
import time
import urllib.request

from playwright.sync_api import sync_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
MC = os.environ.get("MC_DIR", os.path.join(REPO, "..", "..", "04_MinhChung_Demo"))
IMG, LOG = os.path.join(MC, "anh_chup"), os.path.join(MC, "log", "api")
H = os.environ.get("HOST", "localhost")
URL = {
    "master": f"http://{H}:8080", "history": f"http://{H}:18080", "minio": f"http://{H}:9001",
    "kafka_ui": f"http://{H}:8090", "dashboard": f"http://{H}:8501", "api": f"http://{H}:8000",
}
results = []


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())


def shot(page, name, full=True):
    path = os.path.join(IMG, name)
    page.screenshot(path=path, full_page=full)
    results.append(("✓", name))
    print(f"  ✓ {name}")


def step(name):
    """Decorator: chạy 1 bước chụp, lỗi thì ghi nhận rồi đi tiếp."""
    def wrap(fn):
        def run(ctx):
            page = ctx.new_page()
            try:
                fn(page)
            except Exception as e:  # noqa: BLE001
                msg = (str(e).strip().splitlines() or [type(e).__name__])[0][:150]
                results.append(("✗", f"{name}: {msg}"))
                print(f"  ✗ {name}: {msg}")
            finally:
                page.close()
        run.__name__ = name
        return run
    return wrap


# ------------------------------------------------------------------ Spark
@step("spark_master")
def spark_master(p):
    p.goto(URL["master"])
    p.wait_for_selector("text=Alive Workers")
    shot(p, "02_spark_master.png")


@step("spark_history")
def spark_history(p):
    p.goto(URL["history"])
    p.wait_for_selector("table", timeout=20000)
    p.wait_for_timeout(1500)
    shot(p, "17_spark_history.png")


@step("spark_dag")
def spark_dag(p):
    apps = get_json(f"{URL['history']}/api/v1/applications")
    app = next((a for a in apps if a["name"].startswith("02_clean_silver")), None)
    if app is None:
        raise RuntimeError("chưa có job 02_clean_silver trong History Server (chạy make clean trước)")
    sqls = get_json(f"{URL['history']}/api/v1/applications/{app['id']}/sql?details=false")
    longest = max(sqls, key=lambda s: s.get("duration", 0))
    p.set_viewport_size({"width": 1600, "height": 2200})
    p.goto(f"{URL['history']}/history/{app['id']}/SQL/execution/?id={longest['id']}")
    p.wait_for_selector("svg", timeout=30000)
    p.wait_for_timeout(2500)
    shot(p, "06_spark_ui_dag.png")


# ------------------------------------------------------------------ MinIO
def dismiss_license(p):
    """Bản MinIO cộng đồng hiện popup giấy phép AGPL lần đầu -> bấm Acknowledge."""
    btn = p.get_by_role("button", name="Acknowledge")
    if btn.count():
        btn.first.click()
        p.wait_for_timeout(800)


@step("minio")
def minio(p):
    p.goto(URL["minio"])
    p.wait_for_selector("input", timeout=20000)
    user = p.locator("#accessKey, input[name='accessKey'], input[placeholder='Username']").first
    pwd = p.locator("#secretKey, input[name='secretKey'], input[type='password']").first
    user.fill("minioadmin")
    pwd.fill("minioadmin123")
    p.locator("#do-login, button[type='submit'], button:has-text('Login')").first.click()
    p.wait_for_timeout(3000)
    dismiss_license(p)
    # Đi vào thư mục bằng cách BẤM tên thư mục (URL base64 không đổi được trang trên bản console này)
    for path, name in [([], "04a_minio_lake.png"), (["bronze", "events"], "04_minio_bronze.png"),
                       (["gold"], "04b_minio_gold.png")]:
        p.goto(f"{URL['minio']}/browser/lake")
        p.wait_for_timeout(3000)
        dismiss_license(p)
        for seg in path:
            row = p.get_by_text(seg, exact=True).last      # dòng trong bảng (breadcrumb nằm trước)
            try:
                row.click(timeout=8000)
            except Exception:
                enc = base64.b64encode(("/".join(path) + "/").encode()).decode()
                p.goto(f"{URL['minio']}/browser/lake/{enc}")
                print(f"   ! không bấm được '{seg}', thử URL dự phòng")
                break
            p.wait_for_timeout(2500)
        dismiss_license(p)
        shot(p, name, full=False)


# ------------------------------------------------------------------ Kafka UI
@step("kafka_ui")
def kafka_ui(p):
    p.goto(f"{URL['kafka_ui']}/ui/clusters/ecom-lake/all-topics")
    p.wait_for_timeout(4000)
    shot(p, "10_kafka_ui.png", full=False)
    try:
        p.goto(f"{URL['kafka_ui']}/ui/clusters/ecom-lake/all-topics/ecom.events")
        p.wait_for_timeout(3500)
        shot(p, "10b_kafka_topic_events.png", full=False)
    except Exception:  # noqa: BLE001
        pass


# ------------------------------------------------------------------ Dashboard (Streamlit)
HIDE_ST = ("header, [data-testid='stToolbar'], [data-testid='stStatusWidget'], "
           "[data-testid='stDecoration'] {display:none !important}")


def dashboard_tab(p, tab_name, file_name, wait_ms=6000):
    p.set_viewport_size({"width": 1600, "height": 2400})
    p.goto(URL["dashboard"])
    p.wait_for_selector("[role='tab']", timeout=60000)
    p.add_style_tag(content=HIDE_ST)
    p.get_by_role("tab", name=tab_name).click()
    p.wait_for_timeout(wait_ms)            # chờ Streamlit vẽ xong biểu đồ Plotly
    bottom = p.evaluate("""() => {
        const el = document.querySelector("[data-testid='stMainBlockContainer']")
                 || document.querySelector('.block-container') || document.body;
        let b = 0;
        el.querySelectorAll('*').forEach(c => { const r = c.getBoundingClientRect();
            if (r.height > 0 && r.width > 0 && getComputedStyle(c).visibility !== 'hidden') b = Math.max(b, r.bottom); });
        return b; }""")
    h = int(min(max(bottom + 30, 600), 2400))
    p.screenshot(path=os.path.join(IMG, file_name), clip={"x": 0, "y": 0, "width": 1600, "height": h})
    results.append(("✓", file_name))
    print(f"  ✓ {file_name}")


@step("dashboard")
def dashboard(p):
    for tab, f in [("Tổng quan", "07_dashboard_tongquan.png"),
                   ("Danh mục & thương hiệu", "07b_dashboard_danhmuc.png"),
                   ("Khách hàng (RFM)", "08_dashboard_khachhang.png"),
                   ("Đồ thị sản phẩm", "09_dashboard_dothi.png"),
                   ("Chất lượng dữ liệu", "05_chat_luong_du_lieu.png")]:
        dashboard_tab(p, tab, f)


@step("dashboard_realtime")
def dashboard_realtime(p):
    dashboard_tab(p, "Thời gian thực", "12_dashboard_realtime_alert.png", wait_ms=8000)


# ------------------------------------------------------------------ API
def save_api(p, path, img):
    data = get_json(URL["api"] + path)
    os.makedirs(LOG, exist_ok=True)
    fname = path.strip("/").replace("/", "_").replace("?", "_").replace("=", "").replace("&", "_") + ".json"
    with open(os.path.join(LOG, fname), "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    # trang hiển thị JSON đẹp cho ảnh chụp
    html = ("<html><body style='margin:0;background:#fff'>"
            "<div id='card' style='display:inline-block;font-family:Menlo,monospace;font-size:14px;"
            "padding:20px 24px;min-width:560px;max-width:1100px'>"
            f"<div style='color:#555;margin-bottom:10px'>GET {URL['api']}{path}</div>"
            "<pre style='background:#f6f8fa;padding:16px;border-radius:8px;margin:0;white-space:pre-wrap'>"
            f"{json.dumps(data, indent=2, ensure_ascii=False)[:6000]}</pre></div></body></html>")
    p.set_content(html)
    p.locator("#card").screenshot(path=os.path.join(IMG, img))
    results.append(("✓", img))
    print(f"  ✓ {img}")


@step("api")
def api(p):
    p.set_viewport_size({"width": 1400, "height": 1000})
    p.goto(URL["api"] + "/docs")
    p.wait_for_selector(".opblock", timeout=30000)
    p.wait_for_timeout(1500)
    shot(p, "14_api_swagger.png")
    save_api(p, "/health", "14b_api_health.png")
    seg_csv = os.path.join(REPO, "output", "gold", "customer_segments.csv")
    if os.path.exists(seg_csv):
        with open(seg_csv) as f:
            uid = next(csv.DictReader(f))["user_id"]
        save_api(p, f"/customers/{uid}/next-action", "15_api_next_action.png")
    # gợi ý cho sản phẩm trung tâm nhất (PageRank cao nhất) -> có đủ 5 gợi ý, dễ giải thích
    top_csv = os.path.join(REPO, "output", "graph", "top_pagerank.csv")
    rec_csv = os.path.join(REPO, "output", "gold", "recommendations.csv")
    src, col = (top_csv, "id") if os.path.exists(top_csv) else (rec_csv, "product_id")
    if os.path.exists(src):
        with open(src) as f:
            pid = next(csv.DictReader(f))[col]
        save_api(p, f"/products/{pid}/recommendations", "16_api_recommendations.png")
    save_api(p, "/segments", "16b_api_segments.png")
    try:
        save_api(p, "/alerts?rule_id=CHECKOUT_FAILURE&limit=2", "13_api_alerts.png")   # sự cố thật
        save_api(p, "/alerts?limit=3", "13b_api_alerts_moi_nhat.png")                   # 3 cảnh báo mới nhất
    except Exception:  # noqa: BLE001
        pass


# ------------------------------------------------------------------ Terminal (chạy lệnh rồi "chụp")
TERMINAL_CMDS = [
    ("docker compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'", "01_docker_ps"),
    ("make test", "03_make_test"),
    ("make test-apps", "03b_make_test_apps"),
]


def render_terminal(ctx, cmd, out_text, name, max_lines=70):
    import html
    import re
    clean = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", out_text)
    lines = [ln for ln in clean.splitlines() if ln.strip() and " WARN " not in ln and " INFO " not in ln]
    shown = lines[-max_lines:]
    if len(lines) > max_lines:
        shown = [f"... (đã ẩn {len(lines) - max_lines} dòng đầu, xem đầy đủ trong log/terminal/{name}.txt)"] + shown
    body = html.escape("\n".join(shown))
    body = re.sub(r"(PASSED|passed|Up [^\n]*?(?=\s{2}|$)|running)", r"<span style='color:#7ee787'>\1</span>", body)
    body = re.sub(r"(FAILED|failed|ERROR|Exited[^\n]*)", r"<span style='color:#ff7b72'>\1</span>", body)
    page = ctx.new_page()
    page.set_viewport_size({"width": 1500, "height": 900})
    page.set_content(
        "<html><body style='margin:0;background:#fff'><div id='t' style='display:inline-block;margin:0;"
        "background:#1e1e1e;border-radius:10px;overflow:hidden;min-width:900px;max-width:1450px'>"
        "<div style='background:#323232;padding:8px 12px'><span style='color:#ff5f56'>●</span> "
        "<span style='color:#ffbd2e'>●</span> <span style='color:#27c93f'>●</span>"
        "<span style='color:#aaa;font:12px Menlo,monospace;margin-left:12px'>ecom-lake — zsh</span></div>"
        "<pre style='margin:0;padding:14px 18px;color:#e6e6e6;font:13px/1.45 Menlo,monospace;white-space:pre-wrap'>"
        f"<span style='color:#7ee787'>$</span> {html.escape(cmd)}\n{body}</pre></div></body></html>")
    page.locator("#t").screenshot(path=os.path.join(IMG, name + ".png"))
    page.close()
    results.append(("✓", name + ".png"))
    print(f"  ✓ {name}.png")


def terminal(ctx):
    import subprocess
    tdir = os.path.join(MC, "log", "terminal")
    os.makedirs(tdir, exist_ok=True)
    for cmd, name in TERMINAL_CMDS:
        print(f"  … đang chạy: {cmd}")
        try:
            r = subprocess.run(cmd, shell=True, cwd=REPO, capture_output=True, text=True, timeout=1200)
            out = r.stdout + r.stderr
            with open(os.path.join(tdir, name + ".txt"), "w") as f:
                f.write(f"$ {cmd}\n{out}")
            render_terminal(ctx, cmd, out, name)
        except Exception as e:  # noqa: BLE001
            results.append(("✗", f"{name}: {e}"))
            print(f"  ✗ {name}: {e}")


terminal.__name__ = "terminal"


def main():
    os.makedirs(IMG, exist_ok=True)
    mode = sys.argv[1] if len(sys.argv) > 1 else "web"
    steps = {"realtime": [kafka_ui, dashboard_realtime, api],
             "terminal": [terminal],
             "web": [spark_master, spark_history, spark_dag, minio, kafka_ui, dashboard, api]}.get(mode)
    if steps is None:
        raise SystemExit("Chế độ hợp lệ: (không ghi) | realtime | terminal")
    print(f"Lưu ảnh vào: {os.path.abspath(IMG)}")
    t0 = time.time()
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1600, "height": 1000}, device_scale_factor=2,
                                  locale="vi-VN")
        for s in steps:
            print(f"- {s.__name__}")
            s(ctx)
        browser.close()
    ok = sum(1 for r in results if r[0] == "✓")
    print(f"\nXong trong {time.time() - t0:.0f}s: {ok} ảnh thành công, {len(results) - ok} lỗi")
    for r in results:
        if r[0] == "✗":
            print("  ✗", r[1])


if __name__ == "__main__":
    main()
