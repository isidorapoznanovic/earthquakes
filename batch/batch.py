import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, StructField, FloatType, IntegerType, TimestampType, LongType

def write_df(dataframe,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    dataframe.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
    ).mode("overwrite").save()

def preprocess_data(dataframe, table_name):
    dataframe = dataframe.withColumn("state", F.trim(F.col("state")))
    dataframe = dataframe.withColumn("state", F.upper(F.col("state")))

    dataframe = dataframe.drop("data_type")

    dataframe = dataframe.withColumnRenamed("magnitudo", "magnitude")

    
    dataframe.write.mode("overwrite").saveAsTable(table_name)

if __name__ == '__main__':

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
        HDFS_NAMENODE + "/data/Eartquakes-1990-2023.csv")


    preprocess_data(df, "preprocessed_eartquakes")

    df= spark.table("preprocessed_eartquakes")

    print("---------------------Prva dva reda----------------------")
    print(df.take(2))
    print("--------------------------------------------------------")

    print("Picked up data frame...")
    #df.registerTempTable("moja")
    sqlDf = spark.sql("SELECT * FROM preprocessed_eartquakes WHERE tsunami <> 0 ORDER BY significance DESC;")
    sqlDf.show()
    write_df(sqlDf, "eartquakes_tsunami_desc_significance")

    print("--------------------------------------------------------pokazao")

    sqlDf = spark.sql("SELECT AVG(magnitude) FROM preprocessed_eartquakes;")
    sqlDf.show()
    print("--------------------------------------------------------pokazaoAVG")
    sqlDf.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/result/AvgMagnitude.csv", header = 'false')

    sqlDf = spark.sql("SELECT state,\
        COUNT(*) as total_occurrences,\
        COUNT(*) / DATEDIFF(MAX(date), MIN(date)) as frequency_per_day,\
        AVG(magnitude) as average_magnitude\
        FROM preprocessed_eartquakes\
        WHERE state IS NOT NULL\
        GROUP BY state\
        ORDER BY total_occurrences DESC\
        LIMIT 10;")
    write_df(sqlDf, "eartquakes_per_state")

    # sqlDf = spark.sql("SELECT\
    #     state,\
    #     COUNT(*) as total_occurrences,\
    #     AVG(depth) as average_depth,\
    #     MIN(depth) as min_depth,\
    #     MAX(depth) as max_depth,\
    #     percentile_approx(depth, 0.5) as median_depth\
    #     FROM preprocessed_eartquakes\
    #     WHERE state IS NOT NULL\
    #     GROUP BY state\
    #     ORDER BY MAX(depth) DESC;")
    # write_df(sqlDf, "depth_per_state")# Define a window specification partitioned by 'state' for general statistics

    window_spec_median = Window.partitionBy("state").orderBy("depth")
    result_df = df.withColumn("total_occurrences", F.count("*").over(window_spec_median)) \
        .withColumn("average_depth", F.avg("depth").over(window_spec_median)) \
        .withColumn("min_depth", F.min("depth").over(window_spec_median)) \
        .withColumn("max_depth", F.max("depth").over(window_spec_median)) 
    result_df = result_df.select("state", "total_occurrences", "average_depth", "min_depth", "max_depth") \
        .dropDuplicates()
    result_df = result_df.withColumn("max_depth", F.round("max_depth"))
    #result_df = result_df.orderBy(F.desc("max_depth"))

    write_df(result_df, "depth_per_state")

    window_spec = Window.partitionBy("state")
    result_df = df.withColumn("total_earthquakes", F.count("*").over(window_spec)) \
        .withColumn("earthquakes_gt_3", F.sum(F.when(F.col("magnitudo") > 3, 1).otherwise(0)).over(window_spec))
    result_df = result_df.select("state", "total_earthquakes", "earthquakes_gt_3") \
        .dropDuplicates()
    result_df = result_df.withColumn("ratio_gt_3", F.col("earthquakes_gt_3") / F.col("total_earthquakes"))

    write_df(result_df, "mag_gt3_per_state")
