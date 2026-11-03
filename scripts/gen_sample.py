"""
Sinh dữ liệu MẪU cùng định dạng với bộ REES46 (để chạy thử pipeline / để người chấm tái lập
mà không cần tải 5+ GB). Cố ý chèn dữ liệu "bẩn" để kiểm thử tầng silver:
  ~30% thiếu category_code, ~10% thiếu brand, ~0.5% bản ghi trùng, ~0.2% giá = 0,
  ~0.1% thiếu user_id, ~0.05% event_type sai.
Chỉ dùng thư viện chuẩn Python.

  python3 scripts/gen_sample.py --rows 500000 --out data/raw/sample/events_sample.csv
"""
import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

CATEGORIES = [
    "electronics.smartphone", "electronics.audio.headphone", "electronics.video.tv",
    "computers.notebook", "computers.desktop", "appliances.kitchen.refrigerators",
    "appliances.environment.vacuum", "apparel.shoes", "furniture.living_room.sofa",
    "construction.tools.drill", "kids.toys", "auto.accessories.player", "sport.bicycle",
]
BRANDS = ["samsung", "apple", "xiaomi", "huawei", "lg", "sony", "lenovo", "acer",
          "bosch", "philips", "nike", "adidas", "asus", "hp", "oppo"]
PRICE_RANGE = {"electronics": (50, 1500), "computers": (200, 2500), "appliances": (40, 1200),
               "apparel": (10, 200), "furniture": (80, 1500), "construction": (15, 400),
               "kids": (5, 150), "auto": (20, 500), "sport": (100, 1200)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--users", type=int, default=20_000)
    ap.add_argument("--products", type=int, default=3_000)
    ap.add_argument("--start", default="2019-10-01")
    ap.add_argument("--days", type=int, default=31)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/raw/sample/events_sample.csv")
    args = ap.parse_args()

    rnd = random.Random(args.seed)
    products = []
    for i in range(args.products):
        cat = rnd.choice(CATEGORIES)
        lo, hi = PRICE_RANGE[cat.split(".")[0]]
        products.append({
            "product_id": 1_000_000 + i,
            "category_id": 2_053_013_550_000_000_000 + CATEGORIES.index(cat) * 1000,
            "category_code": cat if rnd.random() > 0.30 else "",
            "brand": rnd.choice(BRANDS) if rnd.random() > 0.10 else "",
            "price": round(rnd.uniform(lo, hi), 2),
        })
    weights = [1 / (i + 1) ** 0.8 for i in range(args.products)]  # vài sản phẩm "hot"
    start = datetime.fromisoformat(args.start)
    span = args.days * 86400
    end = start + timedelta(days=args.days)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    n = 0
    buf = []   # gom toàn bộ rồi sắp theo thời gian, giống file gốc (producer cần thứ tự này)

    while n < args.rows:
        # 1 phiên = chuỗi view, đôi khi cart, hiếm khi purchase
        user = 500_000_000 + rnd.randrange(args.users)
        session = str(uuid.UUID(int=rnd.getrandbits(128)))
        t = start + timedelta(seconds=rnd.randrange(span))
        for _ in range(rnd.randint(1, 8)):
            p = rnd.choices(products, weights=weights)[0]
            events = ["view"]
            if rnd.random() < 0.08:
                events.append("cart")
                if rnd.random() < 0.45:
                    events.append("purchase")
            for et in events:
                t += timedelta(seconds=rnd.randint(5, 240))
                if t >= end:                       # không để phiên cuối tràn sang ngày sau khoảng dữ liệu
                    break
                row = [t.strftime("%Y-%m-%d %H:%M:%S") + " UTC", et, p["product_id"],
                       p["category_id"], p["category_code"], p["brand"], p["price"],
                       user, session]
                r = rnd.random()
                if r < 0.002:
                    row[6] = 0.0                       # giá lỗi
                elif r < 0.003:
                    row[7] = ""                        # thiếu user_id
                elif r < 0.0035:
                    row[1] = "VIEWED"                  # event_type sai
                buf.append(row)
                n += 1
                if rnd.random() < 0.005:               # bản ghi trùng
                    buf.append(row)
                    n += 1
                if n >= args.rows:
                    break
            if n >= args.rows:
                break
    buf.sort(key=lambda r: r[0])           # "YYYY-MM-DD HH:MM:SS UTC" sắp chuỗi = sắp thời gian
    with open(args.out, "w", newline="") as f:
        out = csv.writer(f)
        out.writerow(["event_time", "event_type", "product_id", "category_id", "category_code",
                      "brand", "price", "user_id", "user_session"])
        out.writerows(buf)
    print(f"Đã sinh {n:,} dòng -> {args.out} ({os.path.getsize(args.out) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
