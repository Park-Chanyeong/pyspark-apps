from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType


def for_each_batch_func(df: DataFrame, epoch_id):
    print(f'====================================== epoch_id: {epoch_id} start ======================================')
    df.persist()
    print(f'streaming dataframe show()')
    df.show(truncate=False)

    json_to_col_df = df.select(
        col('VALUE'),
        get_json_object(col('VALUE'),'$.name').alias('NAME'),
        get_json_object(col('VALUE'),'$.address.country').alias('COUNTRY'),
        get_json_object(col('VALUE'),'$.address.city').alias('CITY'),
        get_json_object(col('VALUE'),'$.age').cast(IntegerType()).alias('AGE')
    )
    print(f'json_to_col_df.show()')
    json_to_col_df.show(truncate=False)
    df.unpersist()
    print(f'====================================== epoch_id: {epoch_id} end ======================================')

app_name = 'get_json_object'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "lesson.spark-streaming.person_info") \
                .option('startingOffsets','latest') \
                .option('failOnDataLoss','false') \
                .load() \
                .selectExpr(
                    "CAST(key AS STRING) AS KEY",
                    "CAST(value AS STRING) AS VALUE"
                )

query = kafka_source_df.writeStream \
        .foreachBatch(for_each_batch_func) \
        .option("checkpointLocation", f'/home/spark/kafka_offsets/{app_name}') \
        .start()

query.awaitTermination()