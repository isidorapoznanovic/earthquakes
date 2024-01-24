# hdfs dfs -rm -r -f /result
hdfs dfs -rm -r -f /data
# hdfs dfs -mkdir /result
hdfs dfs -mkdir /data
hdfs dfs -put ./data/raw/Eartquakes-1990-2023.csv /data/
