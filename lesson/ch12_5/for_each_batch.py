from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def for_each_batch_func(df:DataFrame, epoch_id):
    print(f'====================================== epoch_id: {epoch_id} start ======================================')
    cnt = df.count()
    print(f'streaming 데이터프레임 건수:{cnt}')
    print(f'streaming 데이터프레임 show()')
    df.show(truncate=False)
    print(f'====================================== epoch_id: {epoch_id} end ======================================')


app_name = 'for_each_batch'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "lesson.spark-streaming.test") \
                .option('maxOffsetsPerTrigger','500') \
                .option('failOnDataLoss','false') \
                .option('startingOffsets','earliest') \
                .load() \
                .selectExpr(
                    "CAST(key AS STRING) AS KEY",
                    "CAST(value AS STRING) AS VALUE"
                )
spark.sql('use linkedin')
postings_df = spark.sql('select * from postings')
print('postings.show()')
postings_df.show()

query = kafka_source_df.writeStream \
        .foreachBatch(for_each_batch_func) \
        .option("checkpointLocation", f'/home/spark/kafka_offsets/{app_name}') \
        .start()

query.awaitTermination()