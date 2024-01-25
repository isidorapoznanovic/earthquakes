import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json, window, avg, expr, count, from_unixtime, explode, split, lit, when, to_timestamp

from pyspark.sql import functions as F

earthquakes = StructType([
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("time", TimestampType(), True),
    StructField("tsunami", StringType(), True),
    StructField("status", StringType(), True),
])

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def write_to_postgresql_avg_magnitude(df,epoch_id):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    USER = "postgres"
    PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver="org.postgresql.Driver",
            dbtable="s_avg_mag_table",
            user=USER,
            password=PASSWORD,
            ) \
    .mode('append') \
    .save()

def write_to_postgresql_avg_all(df,epoch_id):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    USER = "postgres"
    PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    df = df.withColumn("time", col("time").cast("timestamp"))

    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver="org.postgresql.Driver",
            dbtable="s_all_table",
            user=USER,
            password=PASSWORD,
            ) \
    .mode('append') \
    .save()

def write_to_postgresql_num_per_hour_df(df,epoch_id):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    USER = "postgres"
    PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver="org.postgresql.Driver",
            dbtable="s_num_per_hour_table",
            user=USER,
            password=PASSWORD,
            ) \
    .mode('append') \
    .save()

def write_to_postgresql_join(df,epoch_id):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    USER = "postgres"
    PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver="org.postgresql.Driver",
            dbtable="s_join",
            user=USER,
            password=PASSWORD,
            ) \
    .mode('append') \
    .save()

def write_to_postgresql_equator(df,epoch_id):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    USER = "postgres"
    PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver="org.postgresql.Driver",
            dbtable="s_equator",
            user=USER,
            password=PASSWORD,
            ) \
    .mode('append') \
    .save()

def write_to_postgresql_equator_gt3(df,epoch_id):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    USER = "postgres"
    PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    df.write \
    .format('jdbc') \
    .options(url=URL,
            driver="org.postgresql.Driver",
            dbtable="s_equator_gt3",
            user=USER,
            password=PASSWORD,
            ) \
    .mode('append') \
    .save()


if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"
    TOPIC = "earthquakes-topic"
    KAFKA_BROKER = "kafka1:19092"
    TOPIC2 = "earthquakes-topic2"
    KAFKA_BROKER2 = "kafka2:19093"

    spark = SparkSession\
        .builder\
        .appName("StreamingProcessing")\
        .getOrCreate()
    quiet_logs(spark)

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .load()

    df2 = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER2) \
        .option("subscribe", TOPIC2) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), earthquakes).alias("data")) \
        .select("data.*")

    join_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), earthquakes).alias("data")) \
        .select("data.*")

    # Earthquakes on same location collected from different streams one 5h one 15minutes
    joined_streams = join_df.join(df2, "location")

    joined_streams = joined_streams.select(
        join_df["location"].alias("location"),
        join_df["magnitude"].alias("magnitude1"),
        join_df["time"].alias("time1"),
        df2["magnitude"].alias("magnitude2"),
        df2["time"].alias("time2"),
    )

    queryJ = joined_streams \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

    query = df.select(col("value").cast("string"))\
    .writeStream\
    .option("truncate", "false")\
    .format("console")\
    .start()

    query = df2\
    .writeStream\
    .option("truncate", "false")\
    .format("console")\
    .start()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), earthquakes).alias("data")) \
        .select("data.*")

    df = df.withColumn("time", from_unixtime(col("time").cast("long") / 1000))

    # Average magnitude in periods of 30 minutes
    result_df = df \
        .groupBy(window(col("time"), "30 minutes")) \
        .agg(avg("magnitude").alias("avg_magnitude")) \
        .select("window.start", "window.end", "avg_magnitude")

    # Number of earthquakes with magnitude bigger than 3 withing 1 hour
    num_per_hour_df = df \
        .filter(col("magnitude") > 3.0) \
        .groupBy(window(col("time"), "60 minutes")) \
        .agg(count("*").alias("num_earthquakes_mag_gt3")) \
        .select("window.start", "window.end", "num_earthquakes_mag_gt3")

    # Max magnitude earthquake near equator within 5h
    around_equator_per_hour_df = df2 \
        .filter((col("latitude") >= -5) & (col("latitude") <= 5)) \
        .groupBy(window(col("time"), "300 minutes")) \
        .agg(F.max("magnitude").alias("max_magnitude")) \
        .select("window.start", "window.end", col("max_magnitude"))

    
    # Number of earthquakes with magnitude bigger than 4 near equator within 5h
    around_equator_gt3_df = df2 \
        .filter((col("latitude") >= -5) & (col("latitude") <= 5) & (col("magnitude") > 4.0)) \
        .groupBy(window(col("time"), "300 minutes")) \
        .agg(count("*").alias("count_gt3_magnitude_eq")) \
        .select("window.start", "window.end", col("count_gt3_magnitude_eq"))

    postgresql_stream=result_df.writeStream \
        .trigger(processingTime='60 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql_avg_magnitude) \
        .start()

   
    postgresql_stream2=df.writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql_avg_all) \
        .start()

    postgresql_stream2=around_equator_per_hour_df.writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql_equator) \
        .start()
        
    postgresql_stream2=around_equator_gt3_df.writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql_equator_gt3) \
        .start()

    postgresql_stream2=joined_streams.writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode('append') \
        .foreachBatch(write_to_postgresql_join) \
        .start()

    postgresql_stream=num_per_hour_df.writeStream \
        .trigger(processingTime='120 seconds') \
        .outputMode('update') \
        .foreachBatch(write_to_postgresql_num_per_hour_df) \
        .start()

    query = result_df \
        .writeStream \
        .outputMode("complete") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    query2 = num_per_hour_df \
        .writeStream \
        .outputMode("complete") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    query.awaitTermination()
