# -*- coding: utf-8 -*-
"""
项目名称: Steam 游戏应用表多源聚合与 MongoDB 嵌套文档批量导入脚本
运行环境: Python 3.12 + PyMongo + MongoDB 8.0.34
主要功能:
  1. 承接作业一清洗后的应用主表数据 (239,664 款游戏)
  2. 聚合 genres (流派)、categories (功能特性)、developers (开发商) 共 7 张一对多关系表
  3. 组装为包含内嵌数组 (Array) 与内嵌子文档 (platforms) 的 BSON 文档结构
  4. 批量写入 MongoDB (steam.applications) 并精确测量写入耗时 (Velocity 指标)
"""

import csv
import json
import time
from collections import defaultdict
from pymongo import MongoClient


def load_id_name_map(csv_path):
    """加载维度字典表 (id -> name)"""
    mapping = {}
    with open(csv_path, mode="r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["id"]] = row["name"]
    return mapping


def load_app_relations(rel_csv_path, id_field, name_map):
    """加载多对多关联表并聚合为每个 appid 对应的标签数组"""
    app_tags = defaultdict(list)
    with open(rel_csv_path, mode="r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            appid_str = row.get("appid")
            tag_id = row.get(id_field)
            if appid_str and tag_id in name_map:
                try:
                    app_tags[int(appid_str)].append(name_map[tag_id])
                except ValueError:
                    pass
    return app_tags


def main():
    dataset_dir = "/home/john/steam_dataset_2025_csv"
    cleaned_apps_tsv = "/home/john/code/cleaned_applications.tsv"

    print("=" * 68)
    print("       MongoDB 半结构化嵌套文档聚合与批量写入测试")
    print("=" * 68)

    # 1. 加载一对多外围维度表并组装内存映射
    t0 = time.perf_counter()
    print("[INFO] 正在加载并聚合流派 (genres)、分类 (categories)、开发商 (developers) 关联表...")

    genres_map = load_id_name_map(f"{dataset_dir}/genres.csv")
    app_genres = load_app_relations(f"{dataset_dir}/application_genres.csv", "genre_id", genres_map)

    categories_map = load_id_name_map(f"{dataset_dir}/categories.csv")
    app_categories = load_app_relations(f"{dataset_dir}/application_categories.csv", "category_id", categories_map)

    developers_map = load_id_name_map(f"{dataset_dir}/developers.csv")
    app_developers = load_app_relations(f"{dataset_dir}/application_developers.csv", "developer_id", developers_map)

    # 2. 读取作业一清洗后的应用主表并构建嵌套 BSON 文档列表
    docs = []
    with open(cleaned_apps_tsv, mode="r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.rstrip("\r\n").split("\t")
            if len(parts) < 13:
                continue
            try:
                appid = int(parts[0])
            except ValueError:
                continue

            release_date = parts[4].strip('"')
            release_year = int(parts[5]) if parts[5].isdigit() and int(parts[5]) > 0 else None
            metacritic = int(parts[6]) if parts[6].isdigit() and int(parts[6]) > 0 else None
            recs = int(parts[7]) if parts[7].isdigit() else 0
            price_cents = int(parts[8]) if parts[8].isdigit() else 0

            doc = {
                "_id": appid,
                "appid": appid,
                "name": parts[1].strip('"'),
                "type": parts[2].strip('"'),
                "is_free": (parts[3].lower() == "true"),
                "release_date": release_date if release_date else None,
                "release_year": release_year,
                "metacritic_score": metacritic,
                "recommendations_total": recs,
                "price_cents": price_cents,
                "platforms": {
                    "windows": (parts[9].lower() == "true"),
                    "mac": (parts[10].lower() == "true"),
                    "linux": (parts[11].lower() == "true"),
                },
                "genres": app_genres.get(appid, []),
                "categories": app_categories.get(appid, []),
                "developers": app_developers.get(appid, []),
                "short_description": parts[12].strip('"')
            }
            docs.append(doc)

    prep_time = time.perf_counter() - t0
    print(f"[INFO] 7 表关联与 BSON 嵌套文档构建完成: 共 {len(docs)} 条文档 (预处理耗时: {prep_time:.2f}s)")

    # 3. 连接 MongoDB 并测量纯批量写入时间 (Velocity 核心指标)
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client["steam"]
    collection = db["applications"]
    collection.drop()

    print("[INFO] 开始向 MongoDB (steam.applications) 执行批量写入...")
    write_start = time.perf_counter()

    batch_size = 50000
    for i in range(0, len(docs), batch_size):
        collection.insert_many(docs[i:i + batch_size], ordered=False)

    write_elapsed = time.perf_counter() - write_start
    throughput = len(docs) / write_elapsed if write_elapsed > 0 else 0

    print("\n" + "=" * 68)
    print("               MongoDB 批量存储执行结果统计汇总")
    print("=" * 68)
    print(f"目标数据库与集合 : steam.applications")
    print(f"聚合源数据表数量 : 7 张 (主表 + 流派/分类/开发商一对多关系表)")
    print(f"成功写入文档总数 : {len(docs)} 条")
    print(f"MongoDB 写入耗时 : {write_elapsed:.3f} 秒")
    print(f"平均写入吞吐速率 : {throughput:,.0f} 文档/秒")
    print("=" * 68)

    # 4. 抽样打印一条典型嵌套文档验证
    sample = collection.find_one({"_id": 1147860}) or collection.find_one()
    if sample:
        print("\n[INFO] 典型游戏嵌套文档结构预览 (_id = {}):".format(sample["_id"]))
        print(json.dumps(sample, ensure_ascii=False, indent=2))

    client.close()


if __name__ == "__main__":
    main()
