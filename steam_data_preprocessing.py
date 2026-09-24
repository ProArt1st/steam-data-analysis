# -*- coding: utf-8 -*-
"""
项目名称: Steam 数据集分布式预处理流水线
运行环境: Apache Spark 3.4.0 (PySpark) + HDFS
主要功能:
  1. 兼容 Python 3.12+ 与 Spark 3.4.0 的底层类型系统
  2. 游戏表与评测表主键去重与空值过滤
  3. 文本行结构规范化：清除 HTML 标签与回车换行符
  4. 基于文本有效字符比例的噪声过滤算法（剔除字符画与纯标点灌水）
  5. 金融字段规范化（整数美分存储）与时域特征抽取（年月日）
  6. 字段剪枝并输出为 Snappy-Parquet 列式存储
"""

import sys
import time
import types
import typing

# -------------------------------------------------------------------------
# Python 3.12+ 与 Spark 3.4.x 兼容补丁
# 解决 Python 3.12 移除 typing.io 与 typing.re 导致 Spark 启动崩溃的问题
# -------------------------------------------------------------------------
if "typing.io" not in sys.modules:
    _typing_io = types.ModuleType("typing.io")
    _typing_io.BinaryIO = getattr(typing, "BinaryIO", None)
    _typing_io.TextIO = getattr(typing, "TextIO", None)
    sys.modules["typing.io"] = _typing_io
    typing.io = _typing_io

if "typing.re" not in sys.modules:
    _typing_re = types.ModuleType("typing.re")
    _typing_re.Pattern = getattr(typing, "Pattern", None)
    _typing_re.Match = getattr(typing, "Match", None)
    sys.modules["typing.re"] = _typing_re
    typing.re = _typing_re

# 导入 PySpark 核心模块
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, from_unixtime, to_date, 
    year, round as spark_round, when, length, trim,
    coalesce as spark_coalesce, lit
)

def create_spark_session():
    """初始化 Spark 会话并配置运行参数"""
    spark = SparkSession.builder \
        .appName("SteamDataPreprocessing") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_applications(spark, hdfs_base):
    """应用主表数据清洗与规范化"""
    print("[INFO] 开始处理应用主表: applications.csv")
    
    input_path = f"{hdfs_base}/applications/applications.csv"
    output_path = f"{hdfs_base}/cleaned/applications_parquet"

    # 读取原始数据
    df_raw = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(input_path)

    total_raw = df_raw.count()

    # 1. 主键去重
    df_dedup = df_raw.dropDuplicates(["appid"])

    # 2. 基础脏数据过滤（主键或名称非空）
    df_filtered = df_dedup.filter(
        col("appid").isNotNull() & (trim(col("appid")) != "") &
        col("name").isNotNull() & (trim(col("name")) != "")
    )

    # 3. 文本清洗：去除 HTML 标签并将换行符替换为空格
    clean_desc = regexp_replace(
        regexp_replace(spark_coalesce(col("short_description"), lit("")), r"<[^>]+>", " "),
        r"[\r\n\t]+", " "
    )

    # 4. 数据类型转换与特征提取
    df_clean = df_filtered \
        .withColumn("appid", col("appid").cast("long")) \
        .withColumn("is_free", col("is_free").cast("boolean")) \
        .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
        .withColumn("release_year", year(col("release_date"))) \
        .withColumn("metacritic_score", col("metacritic_score").cast("integer")) \
        .withColumn("recommendations_total", col("recommendations_total").cast("long")) \
        .withColumn("price_cents", spark_coalesce(col("mat_final_price").cast("integer"), lit(0))) \
        .withColumn("clean_desc", trim(clean_desc))

    # 5. 字段剪枝
    df_final = df_clean.select(
        "appid", "name", "type", "is_free", 
        "release_date", "release_year", "metacritic_score", 
        "recommendations_total", "price_cents",
        "mat_supports_windows", "mat_supports_mac", "mat_supports_linux",
        col("clean_desc").alias("short_description")
    )

    total_clean = df_final.count()
    dropped_count = total_raw - total_clean
    drop_rate = (dropped_count / total_raw) * 100 if total_raw > 0 else 0.0

    # 写入 HDFS Parquet，控制分块数为 2
    df_final.coalesce(2).write.mode("overwrite").parquet(output_path)
    print(f"[INFO] 应用表清洗完成: 原始 {total_raw} 行, 保留 {total_clean} 行, 剔除 {dropped_count} 行 ({drop_rate:.2f}%)")

    return total_raw, total_clean, df_final

def process_reviews(spark, hdfs_base):
    """评测数据表深度清洗与特征提取"""
    print("[INFO] 开始处理评测大表: reviews.csv")
    
    input_path = f"{hdfs_base}/reviews/reviews.csv"
    output_path = f"{hdfs_base}/cleaned/reviews_parquet"

    df_raw = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(input_path)

    total_raw = df_raw.count()

    # 1. 主键去重
    df_dedup = df_raw.dropDuplicates(["recommendationid"])

    # 2. 基础有效性过滤（主键非空且游玩时间非负）
    df_valid = df_dedup.filter(
        col("recommendationid").isNotNull() &
        col("appid").isNotNull() &
        col("review_text").isNotNull() &
        (col("author_playtime_forever").cast("double") >= 0)
    )

    # 3. 文本信噪比过滤：有效字符（汉字、字母、数字）占比须 >= 30%
    valid_text = regexp_replace(col("review_text"), r"[^\u4e00-\u9fa5a-zA-Z0-9]", "")
    valid_chars_len = length(valid_text)
    total_chars_len = length(trim(col("review_text")))

    df_filtered = df_valid.filter(
        (total_chars_len >= 1) & 
        ((valid_chars_len / total_chars_len) >= 0.30)
    )

    # 4. 文本换行符与制表符替换
    clean_review = regexp_replace(
        regexp_replace(col("review_text"), r"<[^>]+>", " "),
        r"[\r\n\t]+", " "
    )

    # 5. 特征工程转换
    df_clean = df_filtered \
        .withColumn("recommendationid", col("recommendationid").cast("long")) \
        .withColumn("appid", col("appid").cast("long")) \
        .withColumn("created_datetime", from_unixtime(col("timestamp_created"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("review_year", year(to_date(col("created_datetime")))) \
        .withColumn("playtime_hours", spark_round(col("author_playtime_forever").cast("double") / 60.0, 1)) \
        .withColumn("is_positive", when(col("voted_up") == "True", 1).otherwise(0)) \
        .withColumn("votes_up", col("votes_up").cast("integer")) \
        .withColumn("clean_review", trim(clean_review))

    # 6. 字段精简选取
    df_final = df_clean.select(
        "recommendationid", "appid", "author_steamid",
        "language", "is_positive", "playtime_hours", 
        "votes_up", "created_datetime", "review_year",
        col("clean_review").alias("review_text")
    )

    total_clean = df_final.count()
    dropped_count = total_raw - total_clean
    drop_rate = (dropped_count / total_raw) * 100 if total_raw > 0 else 0.0

    # 写入 HDFS Parquet，控制分块数为 4
    df_final.coalesce(4).write.mode("overwrite").parquet(output_path)
    print(f"[INFO] 评测表清洗完成: 原始 {total_raw} 行, 保留 {total_clean} 行, 剔除 {dropped_count} 行 ({drop_rate:.2f}%)")

    return total_raw, total_clean, df_final

def main():
    start_time = time.time()
    spark = create_spark_session()
    hdfs_base = "/data/steam"

    try:
        apps_raw, apps_clean, df_apps = process_applications(spark, hdfs_base)
        revs_raw, revs_clean, df_revs = process_reviews(spark, hdfs_base)

        elapsed = time.time() - start_time
        
        # 终端统计面板输出
        print("\n" + "=" * 65)
        print("                  数据预处理作业执行统计汇总")
        print("=" * 65)
        print(f"执行环境: Apache Spark {spark.version} / HDFS")
        print(f"总执行耗时: {elapsed:.2f} 秒")
        print("-" * 65)
        print("1. 应用主表 (applications):")
        print(f"   - 原始记录总数: {apps_raw} 行")
        print(f"   - 清洗保留记录: {apps_clean} 行")
        print(f"   - 过滤脏记录数: {apps_raw - apps_clean} 行")
        print(f"   - 存储目标路径: {hdfs_base}/cleaned/applications_parquet")
        print("-" * 65)
        print("2. 评测大表 (reviews):")
        print(f"   - 原始记录总数: {revs_raw} 行")
        print(f"   - 清洗保留记录: {revs_clean} 行")
        print(f"   - 过滤脏记录数: {revs_raw - revs_clean} 行")
        print(f"   - 存储目标路径: {hdfs_base}/cleaned/reviews_parquet")
        print("=" * 65)

        # 结构与数据采样展示
        print("\n[INFO] 应用主表清洗样本预览:")
        df_apps.select("appid", "name", "price_cents", "release_year", "metacritic_score").show(3, truncate=25)

        print("[INFO] 评测大表清洗样本预览:")
        df_revs.select("recommendationid", "appid", "language", "is_positive", "playtime_hours", "review_year").show(3, truncate=25)

        print("[INFO] 预处理流程执行完毕。")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
