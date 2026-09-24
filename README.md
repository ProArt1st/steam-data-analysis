# Steam 数据集分布式预处理

本项目为基于 Apache Spark 与 Hadoop HDFS 的大数据分布式预处理工程，主要针对规模约的 Steam 海量多模态数据进行集群内部的清洗、特征提取与存储架构升级。

## 文件说明

* `steam_data_preprocessing.py`：基于 PySpark 3.4.0 的核心预处理脚本，实现文本信噪比（SNR）去噪、主键去重、时域特征抽取及 Parquet 列式存储写入。
* `run_preprocessing.sh`：一键作业提交与 HDFS 产物检查脚本，自动配置本地运行参数并验证落盘状态。

## 运行环境

* 操作系统：Linux (Ubuntu / KUbuntu 等)
* 大数据组件：Hadoop 3.x (HDFS / YARN), Apache Spark 3.4.0
* 运行语言：Python 3.8 - 3.12（内置 Python 3.12 底层类型兼容补丁）

## 核心清洗策略

1. 文本行结构规范化：通过正则表达式消除评论与简介正文内的内嵌换行符（`\r\n`）及 HTML 排版标签，彻底规避 Hadoop 按行读取时的错位断行。
2. 文本信噪比（SNR）过滤：设定有效语义字符（汉字、字母、数字）占比不低于 30% 的阈值规则，系统性切除社区特有的 ASCII 字符画、盲文点阵及纯标点符号发泄灌水，同时保留单字中文评价与简短打分。
3. 金融与时域特征工程：时间戳规范化转换为年月日并抽取年份维度，游戏价格统一规范为整数美分存储，布尔推荐倾向转换为 0/1 数值。
4. 存储架构升级：丢弃无用的大体积海报链接，全量清洗结果直接转为 Snappy 压缩的 Parquet 列式存储写回 HDFS，并主动控制分区数量以避免小文件碎片。

## 快速使用

1. 确保 Hadoop 集群正常启动，且原始数据已上传至 HDFS：
   * 应用表路径：`/data/steam/applications/applications.csv`
   * 评测表路径：`/data/steam/reviews/reviews.csv`

2. 赋予脚本执行权限并一键运行：
   ```bash
   chmod +x run_preprocessing.sh
   ./run_preprocessing.sh
