docker exec -it spark-master bash -c "./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars consumer/postgresql-42.5.1.jar consumer/stream.py"
