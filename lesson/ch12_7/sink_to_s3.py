from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object
from pyspark.sql.types import IntegerType


def for_each_batch_func(df: DataFrame, epoch_id):
    print(f'====================================== epoch_id: {epoch_id} start ======================================')
    df.persist()
    print(f'streaming dataframe show()')
    df.show(truncate=False)

    json_to_col_df = df.select(
        get_json_object('VALUE', '$.name').alias('NAME'),
        get_json_object('VALUE', '$.address.country').alias('COUNTRY'),
        get_json_object('VALUE', '$.address.city').alias('CITY'),
        get_json_object('VALUE', '$.age').cast(IntegerType()).alias('AGE')
    )
    df_cnt = json_to_col_df.count()
    json_to_col_df.write \
                    .format('hive') \
                    .mode("append") \
                    .saveAsTable("lesson.person_info")

    print(f'json_to_col_df 저장 완료 ({df_cnt}건)')
    df.unpersist()
    print(f'====================================== epoch_id: {epoch_id} end ======================================')


app_name = 'sink_to_s3'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

spark.sql('CREATE DATABASE IF NOT EXISTS lesson')
spark.sql('USE lesson')
rslt = spark.sql(f'''CREATE TABLE IF NOT EXISTS person_info(
                name      STRING,
                country   STRING,
                city      STRING,
                age       INT)
              USING hive OPTIONS(fileFormat 'parquet')  
              LOCATION 's3a://datalake-spark-pcy/lesson/person_info'
              '''
                 )
print('Table Create (if not exists) completed ')


kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "lesson.spark-streaming.person_info") \
                .option('startingOffsets','earliest') \
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