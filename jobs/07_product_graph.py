"""
JOB 07 — Graph Computing (Chương 8): đồ thị sản phẩm "thường được mua cùng".

  Đỉnh  = sản phẩm (thuộc tính: brand, cat_l1, giá TB, số lượt mua)
  Cạnh  = 2 sản phẩm cùng được thêm giỏ / mua trong 1 phiên; weight = số phiên chung
Phân tích:
  - Thống kê đồ thị: |V|, |E|, mật độ, bậc trung bình, phân phối bậc
  - PageRank (GraphFrames) -> sản phẩm "trung tâm" của mạng mua sắm
  - PageRank tự cài theo mô hình BSP/Pregel (transforms.pagerank_bsp) -> so sánh với GraphFrames
  - Connected Components -> cụm sản phẩm liên thông
  - Label Propagation -> cộng đồng (community detection)
  - Gợi ý top-5 sản phẩm mua cùng cho mỗi sản phẩm -> phục vụ API ra quyết định
Nếu GraphFrames không khả dụng, job tự dùng phiên bản BSP (PageRank) và bỏ qua CC/LPA.

Chạy:  make graph MIN_WEIGHT=3
"""
import argparse
import json
import os
import time

from pyspark.sql import functions as F

from common import OUTPUT_DIR, get_spark, lake, timed, write_rows_csv
from transforms import co_purchase_edges, pagerank_bsp, to_directed, top_neighbors

JOB = "07_product_graph"
GRAPH_DIR = os.path.join(OUTPUT_DIR, "graph")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-weight", type=int, default=3, help="bỏ cạnh xuất hiện ít hơn N phiên")
    ap.add_argument("--max-products", type=int, default=30)
    ap.add_argument("--pr-iter", type=int, default=10)
    ap.add_argument("--lpa-iter", type=int, default=5)
    args = ap.parse_args()

    spark = get_spark(JOB)
    spark.sparkContext.setCheckpointDir(lake("checkpoints", "graph"))   # bắt buộc cho connectedComponents
    silver = spark.read.parquet(lake("silver", "events"))

    with timed(spark, JOB, "build_edges") as info:
        edges = co_purchase_edges(silver, max_products=args.max_products,
                                  min_weight=args.min_weight).cache()
        info["rows"] = edges.count()

    product_attr = (silver.groupBy("product_id").agg(
                        F.first("brand").alias("brand"), F.first("cat_l1").alias("cat_l1"),
                        F.first("category_code").alias("category_code"),
                        F.round(F.avg("price"), 2).alias("avg_price"),
                        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")))
    ids = edges.select(F.col("src").alias("id")).union(edges.select(F.col("dst").alias("id"))).distinct()
    vertices = ids.join(product_attr.withColumnRenamed("product_id", "id"), "id", "left").cache()
    directed = to_directed(edges).cache()

    # ---------------- thống kê cấu trúc ----------------
    n_v, n_e = vertices.count(), edges.count()
    degree = directed.groupBy("src").agg(F.count("*").alias("degree"),
                                         F.sum("weight").alias("weighted_degree")).withColumnRenamed("src", "id")
    deg_stats = degree.agg(F.avg("degree"), F.max("degree"),
                           F.expr("percentile_approx(degree, 0.5)"),
                           F.expr("percentile_approx(degree, 0.99)")).first()
    stats = {"vertices": n_v, "edges": n_e, "min_weight": args.min_weight,
             "density": round(2 * n_e / (n_v * (n_v - 1)), 8) if n_v > 1 else 0,
             "avg_degree": round(deg_stats[0] or 0, 3), "max_degree": deg_stats[1],
             "median_degree": deg_stats[2], "p99_degree": deg_stats[3]}
    deg_dist = degree.groupBy("degree").count().orderBy("degree")
    write_rows_csv(os.path.join(GRAPH_DIR, "degree_distribution.csv"), deg_dist.collect(), deg_dist.columns)

    # ---------------- PageRank: GraphFrames vs BSP tự cài ----------------
    try:
        from graphframes import GraphFrame
        g = GraphFrame(vertices, directed)
        has_gf = True
    except Exception as e:  # noqa: BLE001
        print(f"[graph] GraphFrames không khả dụng ({e}) -> dùng PageRank BSP")
        has_gf = False

    t = time.perf_counter()
    with timed(spark, JOB, f"pagerank_bsp_{args.pr_iter}iter") as info:
        pr_bsp = pagerank_bsp(directed, iterations=args.pr_iter, checkpoint_every=5).cache()
        info["rows"] = pr_bsp.count()
    stats["pagerank_bsp_seconds"] = round(time.perf_counter() - t, 2)

    if has_gf:
        t = time.perf_counter()
        with timed(spark, JOB, f"pagerank_graphframes_{args.pr_iter}iter") as info:
            pr_gf = g.pageRank(resetProbability=0.15, maxIter=args.pr_iter).vertices.select("id", "pagerank").cache()
            info["rows"] = pr_gf.count()
        stats["pagerank_graphframes_seconds"] = round(time.perf_counter() - t, 2)
        # So khớp 2 cách cài đặt: sai lệch tuyệt đối & độ trùng top-100
        cmp = pr_gf.join(pr_bsp, "id")
        stats["pagerank_max_abs_diff"] = cmp.agg(F.max(F.abs(F.col("pagerank") - F.col("rank")))).first()[0]
        top_gf = {r.id for r in pr_gf.orderBy(F.desc("pagerank")).limit(100).collect()}
        top_bsp = {r.id for r in pr_bsp.orderBy(F.desc("rank")).limit(100).collect()}
        stats["pagerank_top100_overlap"] = len(top_gf & top_bsp)
        pr = pr_gf.withColumnRenamed("pagerank", "rank")
    else:
        pr = pr_bsp

    top_pr = (pr.join(vertices, "id").join(degree, "id")
              .select("id", F.round("rank", 4).alias("pagerank"), "degree", "weighted_degree",
                      "brand", "cat_l1", "category_code", "avg_price", "purchases")
              .orderBy(F.desc("pagerank")).limit(100))
    write_rows_csv(os.path.join(GRAPH_DIR, "top_pagerank.csv"), top_pr.collect(), top_pr.columns)

    # ---------------- Connected Components + Label Propagation ----------------
    if has_gf:
        with timed(spark, JOB, "connected_components") as info:
            cc = g.connectedComponents(algorithm="graphx").select("id", "component").cache()
            info["rows"] = cc.count()
        comp = cc.groupBy("component").agg(F.count("*").alias("size")).orderBy(F.desc("size"))
        comp_rows = comp.collect()
        stats["components"] = len(comp_rows)
        stats["largest_component_size"] = comp_rows[0]["size"] if comp_rows else 0
        stats["largest_component_share"] = round(comp_rows[0]["size"] / n_v, 4) if comp_rows else 0
        write_rows_csv(os.path.join(GRAPH_DIR, "components_top.csv"), comp_rows[:50], comp.columns)

        with timed(spark, JOB, f"label_propagation_{args.lpa_iter}iter") as info:
            lpa = g.labelPropagation(maxIter=args.lpa_iter).select("id", "label").cache()
            info["rows"] = lpa.count()
        # mô tả mỗi cộng đồng bằng danh mục chiếm đa số
        comm = (lpa.join(vertices, "id").groupBy("label").agg(
                    F.count("*").alias("size"),
                    F.mode("cat_l1").alias("main_category"),
                    F.mode("brand").alias("main_brand"),
                    F.sum("purchases").alias("purchases"))
                .orderBy(F.desc("size")))
        comm_rows = comm.collect()
        stats["communities"] = len(comm_rows)
        write_rows_csv(os.path.join(GRAPH_DIR, "communities_top.csv"), comm_rows[:50], comm.columns)
        (vertices.join(pr, "id").join(cc, "id").join(lpa, "id")
         .write.mode("overwrite").parquet(lake("gold", "product_graph_vertices")))
    else:
        vertices.join(pr, "id").write.mode("overwrite").parquet(lake("gold", "product_graph_vertices"))
    edges.write.mode("overwrite").parquet(lake("gold", "product_graph_edges"))

    # ---------------- Gợi ý "thường được mua cùng" ----------------
    with timed(spark, JOB, "recommendations") as info:
        recs = (top_neighbors(edges, k=5)
                .join(vertices.select(F.col("id").alias("recommended_id"),
                                      F.col("brand").alias("rec_brand"),
                                      F.col("category_code").alias("rec_category")), "recommended_id", "left")
                .select("product_id", "rank", "recommended_id", "weight", "rec_brand", "rec_category"))
        recs.write.mode("overwrite").parquet(lake("gold", "recommendations"))
        rec_rows = recs.orderBy("product_id", "rank").collect()
        info["rows"] = len(rec_rows)
    write_rows_csv(os.path.join(OUTPUT_DIR, "gold", "recommendations.csv"), rec_rows, recs.columns)

    os.makedirs(GRAPH_DIR, exist_ok=True)
    with open(os.path.join(GRAPH_DIR, "graph_stats.json"), "w") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False, default=str)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))
    top_pr.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
