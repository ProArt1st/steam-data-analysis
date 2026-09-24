#!/bin/bash
# =========================================================================
# 脚本名称: run_preprocessing.sh
# 功能描述: 提交 Spark 数据清洗作业并检查 HDFS 存储结果
# =========================================================================

# 消除主机名解析告警
export SPARK_LOCAL_IP="127.0.0.1"

echo "========================================================================="
echo "正在提交 Spark 数据预处理作业..."
echo "========================================================================="

# 提交 Spark 任务
spark-submit \
  --master "local[*]" \
  --driver-memory 2g \
  steam_data_preprocessing.py

# 检查执行结果
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================================================="
    echo "Spark 作业执行成功，检查 HDFS 产物目录:"
    echo "========================================================================="
    echo ""
    echo "[HDFS 检查] 应用主表 Parquet 目录:"
    hdfs dfs -ls -h /data/steam/cleaned/applications_parquet
    echo ""
    echo "[HDFS 检查] 评测大表 Parquet 目录:"
    hdfs dfs -ls -h /data/steam/cleaned/reviews_parquet
    echo ""
    echo "[INFO] 预处理产物检查完毕。"
else
    echo "[ERROR] 作业执行失败，请查看上方异常日志。"
fi
