"""
JOB 03 — Analytics: SILVER -> GOLD (bộ KPI bằng Spark SQL).

Bảng GOLD (lưu Parquet trên lake + bản CSV nhỏ trong output/gold cho dashboard/báo cáo):
  daily_kpi          doanh thu, lượt xem/giỏ/mua, người dùng, tỷ lệ chuyển đổi, AOV theo ngày
  category_funnel    phễu view -> cart -> purchase theo danh mục cấp 1
  top_products       100 sản phẩm doanh thu cao nhất
  brand_performance  doanh thu & tỷ lệ chuyển đổi theo thương hiệu (top 50)
  hourly_activity    hoạt động theo giờ x thứ trong tuần (heatmap)
  session_funnel     số phiên có view / cart / purchase

Chạy:  make gold
"""
import os

from common import OUTPUT_DIR, get_spark, lake, timed, write_rows_csv

JOB = "03_kpi_gold"

QUERIES = {
    "daily_kpi": """
        SELECT event_date,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END)      AS views,
               SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END)      AS carts,
               SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)  AS purchases,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END), 2) AS revenue,
               COUNT(DISTINCT user_id)                                  AS active_users,
               COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS buyers,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END), 0), 5) AS view_to_purchase_rate,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END), 0), 2) AS avg_order_value
        FROM events GROUP BY event_date ORDER BY event_date
    """,
    "category_funnel": """
        SELECT cat_l1,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END)     AS views,
               SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END)     AS carts,
               SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS purchases,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END), 2) AS revenue,
               ROUND(SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END), 0), 5) AS view_to_cart,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END), 0), 5) AS cart_to_purchase
        FROM events GROUP BY cat_l1 ORDER BY revenue DESC
    """,
    "top_products": """
        SELECT product_id, FIRST(brand) AS brand, FIRST(category_code) AS category_code,
               COUNT(*) AS purchases, ROUND(SUM(price), 2) AS revenue, ROUND(AVG(price), 2) AS avg_price
        FROM events WHERE event_type='purchase'
        GROUP BY product_id ORDER BY revenue DESC LIMIT 100
    """,
    "brand_performance": """
        SELECT brand,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END)     AS views,
               SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS purchases,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END), 2) AS revenue,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END), 0), 5) AS conversion
        FROM events WHERE brand <> 'unknown'
        GROUP BY brand ORDER BY revenue DESC LIMIT 50
    """,
    "hourly_activity": """
        SELECT day_of_week, event_hour, COUNT(*) AS events,
               SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS purchases
        FROM events GROUP BY day_of_week, event_hour ORDER BY day_of_week, event_hour
    """,
    "session_funnel": """
        SELECT SUM(has_view) AS sessions_with_view, SUM(has_cart) AS sessions_with_cart,
               SUM(has_purchase) AS sessions_with_purchase, COUNT(*) AS total_sessions
        FROM (SELECT user_session,
                     MAX(CASE WHEN event_type='view' THEN 1 ELSE 0 END)     AS has_view,
                     MAX(CASE WHEN event_type='cart' THEN 1 ELSE 0 END)     AS has_cart,
                     MAX(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS has_purchase
              FROM events WHERE user_session IS NOT NULL GROUP BY user_session) s
    """,
}


def main():
    spark = get_spark(JOB)
    spark.read.parquet(lake("silver", "events")).createOrReplaceTempView("events")

    for name, sql in QUERIES.items():
        with timed(spark, JOB, name) as info:
            df = spark.sql(sql)
            df.write.mode("overwrite").parquet(lake("gold", name))
            rows = spark.read.parquet(lake("gold", name)).collect()  # bảng gold nhỏ -> collect an toàn
            info["rows"] = len(rows)
        if name in ("daily_kpi", "hourly_activity"):
            rows = sorted(rows, key=lambda r: tuple(r[c] for c in df.columns[:2]))
        elif "revenue" in df.columns:
            rows = sorted(rows, key=lambda r: -(r["revenue"] or 0))
        write_rows_csv(os.path.join(OUTPUT_DIR, "gold", f"{name}.csv"), rows, df.columns)
        print(f"\n=== {name} ===")
        spark.read.parquet(lake("gold", name)).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
