"""
JOB 04 — Analytics nâng cao: phân khúc khách hàng bằng RFM + K-Means (Spark MLlib, Chương 7).

  Recency   = số ngày từ lần mua cuối đến ngày tham chiếu (ngày cuối dữ liệu + 1)
  Frequency = số phiên (session) có mua hàng
  Monetary  = tổng tiền đã mua
Tiền xử lý: log1p (giảm lệch phải của F, M) -> StandardScaler (đưa 3 chiều về cùng thang đo,
vì K-Means dùng khoảng cách Euclid) -> K-Means.
Chọn k: chạy k = kmin..kmax trên mẫu, ghi Silhouette + WSSSE (elbow) ra output/ml/k_search.csv.

Chạy:  make segment K=4
"""
import argparse
import os
from datetime import timedelta

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F

from common import OUTPUT_DIR, append_csv, get_spark, lake, timed, write_rows_csv
from transforms import build_rfm

JOB = "04_segment_kmeans"


def build_pipeline(k: int) -> Pipeline:
    return Pipeline(stages=[
        VectorAssembler(inputCols=["r_log", "f_log", "m_log"], outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        KMeans(k=k, seed=42, featuresCol="features", predictionCol="segment", maxIter=30),
    ])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=4)
    ap.add_argument("--kmin", type=int, default=2)
    ap.add_argument("--kmax", type=int, default=8)
    ap.add_argument("--sample", type=float, default=0.3, help="tỷ lệ mẫu khi dò k")
    ap.add_argument("--skip-search", action="store_true")
    args = ap.parse_args()

    spark = get_spark(JOB)
    silver = spark.read.parquet(lake("silver", "events"))
    ref_date = silver.agg(F.max("event_date")).first()[0] + timedelta(days=1)

    with timed(spark, JOB, "build_rfm") as info:
        rfm = build_rfm(silver, ref_date).cache()
        info["rows"] = rfm.count()
    print(f"Ngày tham chiếu: {ref_date}, số khách đã mua: {info['rows']:,}")
    rfm.select("recency", "frequency", "monetary").summary("min", "25%", "50%", "75%", "max", "mean").show()

    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="segment")
    if not args.skip_search:
        ks_path = os.path.join(OUTPUT_DIR, "ml", "k_search.csv")
        if os.path.exists(ks_path):
            os.remove(ks_path)
        sample = rfm.sample(fraction=args.sample, seed=42).cache()
        for k in range(args.kmin, args.kmax + 1):
            with timed(spark, JOB, f"k_search_k{k}") as info:
                model = build_pipeline(k).fit(sample)
                sil = evaluator.evaluate(model.transform(sample))
                wssse = model.stages[-1].summary.trainingCost
                info["rows"] = k
            append_csv(ks_path, {"k": k, "silhouette": round(sil, 4), "wssse": round(wssse, 2)})
            print(f"k={k}: silhouette={sil:.4f} wssse={wssse:.2f}")
        sample.unpersist()

    with timed(spark, JOB, f"fit_final_k{args.k}") as info:
        model = build_pipeline(args.k).fit(rfm)
        pred = model.transform(rfm).cache()
        info["rows"] = pred.count()
    sil = evaluator.evaluate(pred)
    print(f"Silhouette (k={args.k}, toàn bộ): {sil:.4f}")
    append_csv(os.path.join(OUTPUT_DIR, "ml", "final_model.csv"),
               {"k": args.k, "silhouette_full": round(sil, 4), "customers": info["rows"]})

    profile = (pred.groupBy("segment").agg(
                   F.count("*").alias("customers"),
                   F.round(F.avg("recency"), 1).alias("avg_recency_days"),
                   F.round(F.avg("frequency"), 2).alias("avg_frequency"),
                   F.round(F.avg("monetary"), 2).alias("avg_monetary"),
                   F.round(F.sum("monetary"), 2).alias("total_revenue"))
               .orderBy("segment"))
    profile.show(truncate=False)
    write_rows_csv(os.path.join(OUTPUT_DIR, "gold", "segment_profile.csv"),
                   profile.collect(), profile.columns)

    seg = pred.select("user_id", "recency", "frequency", "monetary", "segment")
    seg.write.mode("overwrite").parquet(lake("gold", "customer_segments"))
    # Bản CSV cho API ra quyết định (1 file, vài MB với ~300k khách)
    write_rows_csv(os.path.join(OUTPUT_DIR, "gold", "customer_segments.csv"),
                   seg.toLocalIterator(), seg.columns)
    model.write().overwrite().save(lake("models", f"rfm_kmeans_k{args.k}"))
    spark.stop()


if __name__ == "__main__":
    main()
