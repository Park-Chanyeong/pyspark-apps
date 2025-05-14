from pyspark.sql import SparkSession

app_name = 'kafka_streaming_casting'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "lesson.spark-streaming.test") \
                .load()

kafka_source_df = kafka_source_df.selectExpr(
                    "CAST(key AS STRING) AS KEY",
                    "CAST(value AS STRING) AS VALUE"
                )

query = kafka_source_df.writeStream \
        .format('console') \
        .option("checkpointLocation", f'/home/spark/kafka_offsets/{app_name}') \
        .option("truncate", "false") \
        .start()

query.awaitTermination()