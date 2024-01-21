import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, StructField, FloatType, IntegerType, TimestampType, LongType

print("Spark job started")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("app1").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf) \
                            .enableHiveSupport() \
                            .getOrCreate()

print("Trying to read from file..." + HDFS_NAMENODE)

customSchema = StructType([
    StructField("time", LongType(), True),
    StructField("place", StringType(), True),
    StructField("status", StringType(), True),
    StructField("tsunami", FloatType(), True),
    StructField("significance", IntegerType(), True),
    StructField("data_type", StringType(), True),
    StructField("magnitudo", FloatType(), True),
    StructField("state", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("depth", FloatType(), True),
    StructField("date", TimestampType(), True),
])

df = spark.read.format("csv").option("header","true").schema(customSchema).load(
    HDFS_NAMENODE + "/test/Eartquakes-1990-2023.csv")

print("---------------------Prva dva reda----------------------")
print(df.take(2))
print("--------------------------------------------------------")

print("Picked up data frame...")
df.registerTempTable("moja")
sqlDf = spark.sql("SELECT * FROM moja WHERE tsunami <> 0 ORDER BY significance DESC;")
sqlDf.show()
sqlDf.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/results/TsunamiEartquakes.csv", header = 'true')
print("--------------------------------------------------------pokazao")

sqlDf = spark.sql("SELECT AVG(magnitudo) FROM moja;")
sqlDf.show()
print("--------------------------------------------------------pokazaoAVG")
sqlDf.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/results/AvgMagnitude.csv", header = 'false')

print("Saved csvs")

#visina 75
#sirina 90
#dubina 60