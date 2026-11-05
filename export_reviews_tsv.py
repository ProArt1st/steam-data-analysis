# -*- coding: utf-8 -*-
"""
项目名称: Steam 清洗后评测数据导出 TSV 工具 (承接作业一 Parquet 产物)
运行环境: Apache Spark 3.4.0 (PySpark) + HDFS
主要功能:
  1. 读取作业一清洗后的 HDFS Parquet 评测表 (/data/steam/cleaned/reviews_parquet)
  2. 提取结构化与时序统计字段，输出为无表头标准 TSV 格式 (/data/steam/hbase_input/reviews.tsv)
  3. 供 HBase ImportTsv 批量导入及后续 MapReduce 离线统计作业使用
"""

import sys
import types
import typing

# -------------------------------------------------------------------------
# Python 3.12+ 与 Spark 3.4.x 兼容补丁
# -------------------------------------------------------------------------
if "typing.io" not in sys.modules:
    _t = types.ModuleType("typing.io")
    _t.BinaryIO = getattr(typing, "BinaryIO", None)
    _t.TextIO = getattr(typing, "TextIO", None)
    sys.modules["typing.io"] = _t
    typing.io = _t

if "typing.re" not in sys.modules:
    _r = types.ModuleType("typing.re")
    _r.Pattern = getattr(typing, "Pattern", None)
    _r.Match = getattr(typing, "Match", None)
    sys.modules["typing.re"] = _r
    typing.re = _r

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("ExportReviewsTSV") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_parquet = "hdfs://localhost:9000/data/steam/cleaned/reviews_parquet"
    output_tsv_dir = "hdfs://localhost:9000/data/steam/hbase_input"

    df = spark.read.parquet(input_parquet)

    # 选取 HBase RowKey (recommendationid) 及 meta / stat 列族对应字段
    df_tsv = df.select(
        "recommendationid",
        "appid",
        "author_steamid",
        "language",
        "is_positive",
        "playtime_hours",
        "votes_up",
        "created_datetime",
        "review_year"
    )

    df_tsv.coalesce(1).write \
        .mode("overwrite") \
        .option("sep", "\t") \
        .option("header", "false") \
        .csv(output_tsv_dir)

    print(f"[INFO] TSV 导出完成，总记录数: {df_tsv.count()} 行")
    spark.stop()


if __name__ == "__main__":
    main()
