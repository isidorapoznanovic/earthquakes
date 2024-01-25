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

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

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

    
    quiet_logs(spark)

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

    print("Raw data picked up")

    preprocess_data(df, "preprocessed_eartquakes")

    print("Preprocessing done")

    df= spark.table("preprocessed_eartquakes")

    # ### QUERY 1
    sqlDf = spark.sql("SELECT * \
        FROM preprocessed_eartquakes \
        WHERE tsunami <> 0 \
        ORDER BY significance DESC;")
    write_df(sqlDf, "b_eartquakes_tsunami_desc_significance")
    print("Query 1 done")


    # ### QUERY 2
    sqlDf = spark.sql("SELECT AVG(magnitude) \
        FROM preprocessed_eartquakes;")
    write_df(sqlDf, "b_all_eartquakes_average_magnitude")
    print("Query 2 done")


    # ### QUERY 3
    # ### Frequency od earthquakes per day per state and average magnitude
    sqlDf = spark.sql("SELECT state,\
        COUNT(*) as total_occurrences,\
        COUNT(*) / DATEDIFF(MAX(date), MIN(date)) as frequency_per_day,\
        AVG(magnitude) as average_magnitude\
        FROM preprocessed_eartquakes\
        WHERE state IS NOT NULL\
        GROUP BY state\
        ORDER BY total_occurrences DESC\
        LIMIT 50;")
    write_df(sqlDf, "b_eartquakes_per_state")
    print("Query 3 done")


    # ### QUERY 4
    # ### Total occurrences number, average depth, min depth, max depth of earthquakes per state
    window_spec_median = Window.partitionBy("state").orderBy("depth")
    result_df = df.withColumn("total_occurrences", F.count("*").over(window_spec_median)) \
        .withColumn("average_depth", F.avg("depth").over(window_spec_median)) \
        .withColumn("min_depth", F.min("depth").over(window_spec_median)) \
        .withColumn("max_depth", F.max("depth").over(window_spec_median)) 
    result_df = result_df.select("state", "total_occurrences", "average_depth", "min_depth", "max_depth") \
        .dropDuplicates()
    result_df = result_df.withColumn("max_depth", F.round("max_depth"))

    write_df(result_df, "b_depth_per_state")
    print("Query 4 done")


    # ### QUERY 5
    # ### Percent of earthquakes with magnitude bigger than 3 in states
    window_spec = Window.partitionBy("state")
    result_df = df.withColumn("total_earthquakes", F.count("*").over(window_spec)) \
        .withColumn("earthquakes_gt_3", F.sum(F.when(F.col("magnitude") > 3, 1).otherwise(0)).over(window_spec))
    result_df = result_df.select("state", "total_earthquakes", "earthquakes_gt_3") \
        .dropDuplicates()
    result_df = result_df.withColumn("ratio_gt_3", F.col("earthquakes_gt_3") / F.col("total_earthquakes"))

    write_df(result_df, "b_mag_gt3_per_state")
    print("Query 5 done")


    # ### QUERY 6
    # ### Yearly occurance of earthquake, earthquake with tsunami 
    result_df = df.withColumn("year", F.year("date"))
    result_df = result_df.groupBy("year").agg(F.count("*").alias("total_earthquakes"),
            F.sum(F.when(F.col("tsunami") == 1, 1).otherwise(0)).alias("earthquakes_with_tsunami"))
    result_df = result_df.withColumn("percentage_with_tsunami", (F.col("earthquakes_with_tsunami") / F.col("total_earthquakes")) * 100)

    write_df(result_df, "b_yearly_total_earthquakes")
    print("Query 6 done")

    # ### QUERY 7
    # ### Average, max magnitude and number of earthquakes with magnitude bigger than 6 for each month in South and North hemisphere
    result_df = df.withColumn("hemisphere", F.when(F.col("latitude") >= 0, "North").otherwise("South"))
    result_df = result_df.withColumn("month", F.month("date"))
    result_df = result_df.groupBy("month", "hemisphere") \
        .agg(F.avg("magnitude").alias("average_magnitude"),
        F.max("magnitude").alias("max_magnitude"),
        F.sum(F.when(F.col("magnitude") > 6, 1).otherwise(0)).alias("count_gt_6_magnitude")
    )

    write_df(result_df, "b_montly_earthquake_magnitude_hemispheres")
    print("Query 7 done")

    # ### QUERY 8
    # ### Pair of earthquakes that occured in 15 minutes in same area
    result_df = df.filter((F.year("date") == 2019)&(F.month("date") == 1))
    joined_df = result_df.alias("e1").join(df.alias("e2"),
                                (F.col("e1.latitude") - F.col("e2.latitude")).between(-0.1, 0.1) &
                                (F.col("e1.longitude") - F.col("e2.longitude")).between(-0.1, 0.1) &
                                (F.col("e1.date") != F.col("e2.date")) &
                                (F.abs(F.unix_timestamp("e1.date") - F.unix_timestamp("e2.date")) <= 900),  # Within 15 minutes
                                "inner")

    result_df = joined_df.select(
        F.when(F.col("e1.date") < F.col("e2.date"), F.col("e1.place")).otherwise(F.col("e2.place")).alias("place1"),
        F.when(F.col("e1.date") < F.col("e2.date"), F.col("e2.place")).otherwise(F.col("e1.place")).alias("place2"),
        F.least("e1.date", "e2.date").alias("date1"),
        F.greatest("e1.date", "e2.date").alias("date2"),
        F.when(F.col("e1.date") < F.col("e2.date"), F.col("e1.magnitude")).otherwise(F.col("e2.magnitude")).alias("magnitude1"),
        F.when(F.col("e1.date") < F.col("e2.date"), F.col("e2.magnitude")).otherwise(F.col("e1.magnitude")).alias("magnitude2")
    ).distinct()

    write_df(result_df, "b_pair_of_earthquakes")
    print("Query 8 done")

    # ### QUERY 9
    # ### Average occurrences per month in states near equator
    equator_df = df.filter((F.col("latitude") >= -5) & (F.col("latitude") <= 5))
    result_df = equator_df.withColumn("month", F.month("date")) \
        .withColumn("state", F.col("state"))
    result_df = result_df.groupBy("month", "state") \
        .agg(F.count("*").alias("occurrences"))
    result_df = result_df.groupBy("state") \
        .agg(F.sum("occurrences").alias("total_occurrences"))
    result_df = result_df.withColumn("average_occurrences_per_month", F.col("total_occurrences") / 12)

    write_df(result_df, "b_equator_earthquakes")
    print("Query 9 done")

    # ### QUERY 10
    # ### Magnitude and coordinates of all earthquakes happend on 2.8.2000. year 
    august_2_2000_df = df.filter((F.year("date") == 2000) & (F.month("date") == 8) & (F.dayofmonth("date") == 2))
    result_df = august_2_2000_df.select(
        "latitude",
        "longitude",
        "depth",
        "magnitude"
    )
    write_df(result_df, "b_birthday_earthquakes")
    print("Query 10 done")

    # ### QUERY 11
    # ### Bigest aftershock

    df_m = df.filter(F.col("magnitude") >= 8)
    df_a = df.filter(F.col("magnitude") < 8)
    joined_df = df_m.alias("main").join(df_a.alias("a"),
                                (F.col("main.latitude") - F.col("a.latitude")).between(-0.1, 0.1) &
                                (F.col("main.longitude") - F.col("a.longitude")).between(-0.1, 0.1) &
                                (F.col("main.date") != F.col("a.date")) &
                                ((F.unix_timestamp("a.date") - F.unix_timestamp("main.date")) > 0) &
                                ((F.unix_timestamp("a.date") - F.unix_timestamp("main.date")) <= 18000),  # Within 5 hours
                                "inner")
    
    joined_df = joined_df.select(
        "main.date",
        "main.magnitude",
        "main.place",
        "a.date",
        "a.magnitude",
        "a.place"
    )

    window_spec = Window.partitionBy("main.date", "main.magnitude", "main.place")
    result_df = joined_df.withColumn("max_magnitude2", F.max("a.magnitude").over(window_spec))
    result_df = result_df.filter(F.col("a.magnitude") == F.col("max_magnitude2"))

    final_result_df = result_df.select(
        "main.date",
        "main.magnitude",
        "main.place",
        "a.date",
        "a.magnitude",
        "a.place"
    )

    result_df = final_result_df.select(
        F.when(F.col("main.date") < F.col("a.date"), F.col("main.place")).otherwise(F.col("a.place")).alias("place_main"),
        F.when(F.col("main.date") < F.col("a.date"), F.col("a.place")).otherwise(F.col("main.place")).alias("place_aftershock"),
        F.least("main.date", "a.date").alias("date_main"),
        F.greatest("main.date", "a.date").alias("date_afershock"),
        F.greatest("main.magnitude", "a.magnitude").alias("main_magnitude"),
        F.least("main.magnitude", "a.magnitude").alias("aftershock_magnitude"),
    ).distinct()

    write_df(result_df, "b_aftershock_earthquakes")
    print("Query 11 done")


    # ### QUERY 12
    # ### Bigest forshock

    df_m = df.filter(F.col("magnitude") >= 8)
    df_f = df.filter(F.col("magnitude") < 8)
    joined_df = df_m.alias("main").join(df_f.alias("f"),
                                (F.col("main.latitude") - F.col("f.latitude")).between(-0.1, 0.1) &
                                (F.col("main.longitude") - F.col("f.longitude")).between(-0.1, 0.1) &
                                (F.col("main.date") != F.col("f.date")) &
                                ((F.unix_timestamp("main.date") - F.unix_timestamp("f.date")) > 0) &
                                ((F.unix_timestamp("main.date") - F.unix_timestamp("f.date")) <= 18000),  # Within 5 hours
                                "inner")
    
    joined_df = joined_df.select(
        "main.date",
        "main.magnitude",
        "main.place",
        "f.date",
        "f.magnitude",
        "f.place"
    )

    window_spec = Window.partitionBy("main.date", "main.magnitude", "main.place")
    result_df = joined_df.withColumn("max_magnitude2", F.max("f.magnitude").over(window_spec))
    result_df = result_df.filter(F.col("f.magnitude") == F.col("max_magnitude2"))

    final_result_df = result_df.select(
        "main.date",
        "main.magnitude",
        "main.place",
        "f.date",
        "f.magnitude",
        "f.place"
    )

    result_df = joined_df.select(
        F.when(F.col("main.date") > F.col("f.date"), F.col("main.place")).otherwise(F.col("f.place")).alias("place_main"),
        F.when(F.col("main.date") > F.col("f.date"), F.col("f.place")).otherwise(F.col("main.place")).alias("place_forshock"),
        F.greatest("main.date", "f.date").alias("date_main"),
        F.least("main.date", "f.date").alias("date_forshock"),
        F.greatest("main.magnitude", "f.magnitude").alias("main_magnitude"),
        F.least("main.magnitude", "f.magnitude").alias("forshock_magnitude"),
    ).distinct()

    write_df(result_df, "b_forshock_earthquakes")
    print("Query 12 done")

    print("Batch data processing done")