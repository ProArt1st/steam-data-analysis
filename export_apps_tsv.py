# -*- coding: utf-8 -*-
import sys, types, typing
if "typing.io" not in sys.modules:
    _t = types.ModuleType("typing.io"); _t.BinaryIO = getattr(typing, "BinaryIO", None); _t.TextIO = getattr(typing, "TextIO", None)
    sys.modules["typing.io"] = _t; typing.io = _t
if "typing.re" not in sys.modules:
    _r = types.ModuleType("typing.re"); _r.Pattern = getattr(typing, "Pattern", None); _r.Match = getattr(typing, "Match", None)
    sys.modules["typing.re"] = _r; typing.re = _r

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit, col

spark = SparkSession.builder.appName("ExportAppsTSV").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://localhost:9000/data/steam/cleaned/applications_parquet")
df_tsv = df.select(
    "appid", "name", "type", "is_free",
    coalesce(col("release_date").cast("string"), lit("")).alias("release_date"),
    coalesce(col("release_year").cast("string"), lit("0")).alias("release_year"),
    coalesce(col("metacritic_score").cast("string"), lit("0")).alias("metacritic_score"),
    coalesce(col("recommendations_total").cast("string"), lit("0")).alias("recommendations_total"),
    coalesce(col("price_cents").cast("string"), lit("0")).alias("price_cents"),
    coalesce(col("mat_supports_windows").cast("string"), lit("False")).alias("win"),
    coalesce(col("mat_supports_mac").cast("string"), lit("False")).alias("mac"),
    coalesce(col("mat_supports_linux").cast("string"), lit("False")).alias("linux"),
    coalesce(col("short_description"), lit("")).alias("short_description")
)
df_tsv.coalesce(1).write.mode("overwrite").option("sep", "\t").option("header", "false").csv("hdfs://localhost:9000/data/steam/mongo_input")
print("Exported applications rows:", df_tsv.count())
spark.stop()
