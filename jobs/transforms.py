"""
Các phép biến đổi thuần (DataFrame -> DataFrame), tách khỏi job để KIỂM THỬ được bằng pytest.
Job chỉ lo đọc/ghi và đo thời gian; logic nghiệp vụ nằm hết ở đây.
"""
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

VALID_TYPES = ["view", "cart", "remove_from_cart", "purchase"]
DEDUP_KEYS = ["event_ts", "event_type", "product_id", "user_id", "user_session"]
SILVER_COLUMNS = ["event_ts", "event_date", "event_hour", "day_of_week", "event_type",
                  "product_id", "category_id", "category_code", "cat_l1", "cat_l2",
                  "brand", "price", "user_id", "user_session"]

# Schema bản tin JSON trên Kafka topic ecom.events (xem docs/DATA_CONTRACT.md)
KAFKA_EVENT_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
    StructField("produced_at_ms", LongType()),   # thời điểm producer gửi -> đo độ trễ end-to-end
])


# ---------------------------------------------------------------- Silver (làm sạch)
def parse_event_ts(col_name: str = "event_time"):
    """'2019-10-01 00:00:00 UTC' -> timestamp (null nếu sai định dạng)."""
    return F.to_timestamp(F.regexp_replace(F.col(col_name), " UTC$", ""), "yyyy-MM-dd HH:mm:ss")


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Gắn cờ vi phạm R1–R3 cho từng dòng, không loại dòng nào."""
    return (df
            .withColumn("event_ts", parse_event_ts())
            .withColumn("event_type", F.lower(F.trim("event_type")))
            .withColumn("f_missing_key",
                        F.col("user_id").isNull() | F.col("product_id").isNull()
                        | F.col("event_ts").isNull() | F.col("event_type").isNull())
            .withColumn("f_bad_type", F.coalesce(~F.col("event_type").isin(VALID_TYPES), F.lit(True)))
            .withColumn("f_bad_price", F.col("price").isNull() | (F.col("price") <= 0))
            .withColumn("f_invalid", F.col("f_missing_key") | F.col("f_bad_type") | F.col("f_bad_price")))


def quality_profile(flagged: DataFrame) -> dict:
    """Đếm vi phạm theo từng luật trong 1 lần quét."""
    return flagged.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.sum(F.col("f_missing_key").cast("int")).alias("r1_missing_key"),
        F.sum(F.col("f_bad_type").cast("int")).alias("r2_bad_event_type"),
        F.sum(F.col("f_bad_price").cast("int")).alias("r3_bad_price"),
        F.sum(F.col("f_invalid").cast("int")).alias("invalid_rows_r1_r3"),
        F.sum(F.col("brand").isNull().cast("int")).alias("null_brand"),
        F.sum(F.col("category_code").isNull().cast("int")).alias("null_category_code"),
        F.sum(F.col("user_session").isNull().cast("int")).alias("null_user_session"),
    ).first().asDict()


def _blank_to(col, default: str):
    """null hoặc chuỗi rỗng -> giá trị mặc định."""
    return F.when(col.isNull() | (col == ""), F.lit(default)).otherwise(col)


def clean_events(flagged: DataFrame) -> DataFrame:
    """Loại dòng vi phạm R1–R3, khử trùng (R4) và chuẩn hóa -> schema silver."""
    return (flagged
            .filter(~F.col("f_invalid"))
            .dropDuplicates(DEDUP_KEYS)
            .withColumn("brand", _blank_to(F.lower(F.trim("brand")), "unknown"))
            .withColumn("category_code", _blank_to(F.trim("category_code"), "unknown"))
            .withColumn("cat_l1", F.split("category_code", r"\.").getItem(0))
            .withColumn("cat_l2", F.coalesce(F.split("category_code", r"\.").getItem(1), F.lit("unknown")))
            .withColumn("event_hour", F.hour("event_ts"))
            .withColumn("day_of_week", F.date_format("event_ts", "E"))
            .withColumn("event_date", F.to_date("event_ts"))
            .select(*SILVER_COLUMNS))


# ---------------------------------------------------------------- RFM
def build_rfm(silver: DataFrame, ref_date: date) -> DataFrame:
    """RFM cho khách đã mua. ref_date = ngày cuối của dữ liệu + 1."""
    buys = silver.filter(F.col("event_type") == "purchase")
    return (buys.groupBy("user_id").agg(
                F.datediff(F.lit(ref_date), F.max("event_date")).alias("recency"),
                F.countDistinct("user_session").alias("frequency"),
                F.round(F.sum("price"), 2).alias("monetary"))
            .withColumn("r_log", F.log1p("recency"))
            .withColumn("f_log", F.log1p("frequency"))
            .withColumn("m_log", F.log1p("monetary")))


# ---------------------------------------------------------------- Graph
def co_purchase_edges(silver: DataFrame, event_types=("cart", "purchase"),
                      max_products: int = 30, min_weight: int = 2) -> DataFrame:
    """
    Cạnh vô hướng (src < dst) giữa 2 sản phẩm cùng xuất hiện trong 1 phiên có cart/purchase.
    weight = số phiên chứa cả 2 sản phẩm. Phiên có quá nhiều sản phẩm (> max_products)
    bị bỏ qua để tránh bùng nổ số cặp O(n^2) và nhiễu từ bot/khách lướt.
    """
    sess = (silver.filter(F.col("event_type").isin(list(event_types)))
            .groupBy("user_session").agg(F.collect_set("product_id").alias("items"))
            .filter((F.size("items") >= 2) & (F.size("items") <= max_products)))
    a = sess.select("user_session", F.explode("items").alias("src"))
    b = sess.select("user_session", F.explode("items").alias("dst"))
    return (a.join(b, "user_session")
            .filter(F.col("src") < F.col("dst"))
            .groupBy("src", "dst").agg(F.countDistinct("user_session").alias("weight"))
            .filter(F.col("weight") >= min_weight))


def to_directed(edges: DataFrame) -> DataFrame:
    """Nhân đôi cạnh vô hướng thành 2 cạnh có hướng (cần cho PageRank)."""
    return edges.select("src", "dst", "weight").unionByName(
        edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"), "weight"))


def pagerank_bsp(directed_edges: DataFrame, iterations: int = 10, reset: float = 0.15,
                 checkpoint_every: int = 0) -> DataFrame:
    """
    PageRank theo mô hình vertex-centric / BSP (Pregel) viết bằng DataFrame:
      superstep: mỗi đỉnh gửi rank/outdeg cho hàng xóm (message passing = join + groupBy),
      đỉnh gom message: rank = reset + (1-reset) * sum(msg).   (chuẩn hóa tổng = N như GraphX)
    Không trọng số, để so sánh trực tiếp với GraphFrames.pageRank.
    """
    edges = directed_edges.select("src", "dst").distinct()
    outdeg = edges.groupBy("src").agg(F.count("*").alias("outdeg"))
    vertices = edges.select(F.col("src").alias("id")).union(edges.select(F.col("dst").alias("id"))).distinct()
    ranks = vertices.withColumn("rank", F.lit(1.0))
    e = edges.join(outdeg, "src").cache()
    for i in range(iterations):
        msgs = (e.join(ranks.withColumnRenamed("id", "src"), "src")
                .groupBy("dst").agg(F.sum(F.col("rank") / F.col("outdeg")).alias("msg")))
        ranks = (vertices.join(msgs.withColumnRenamed("dst", "id"), "id", "left")
                 .select("id", (F.lit(reset) + F.lit(1 - reset) * F.coalesce("msg", F.lit(0.0))).alias("rank")))
        if checkpoint_every and (i + 1) % checkpoint_every == 0:
            ranks = ranks.localCheckpoint()   # cắt lineage dài sau nhiều vòng lặp
    return ranks


def top_neighbors(edges: DataFrame, k: int = 5) -> DataFrame:
    """Gợi ý 'thường được mua cùng': k hàng xóm có trọng số lớn nhất của mỗi sản phẩm."""
    w = Window.partitionBy("product_id").orderBy(F.desc("weight"), F.asc("recommended_id"))
    both = to_directed(edges).select(F.col("src").alias("product_id"),
                                     F.col("dst").alias("recommended_id"), "weight")
    return (both.withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= k))
