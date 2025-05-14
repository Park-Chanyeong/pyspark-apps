from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType


schema = StructType(
    [
        StructField('name', StringType(), False),
        StructField('address', StructType(
            [
                StructField('country', StringType(), False),
                StructField('city', StringType(), False),
            ]
        ), False),
        StructField('age', IntegerType(), False),
        StructField('hobbies', ArrayType(StringType()), False)
    ]
)


def for_each_batch_func(df: DataFrame, epoch_id):
    print(f'====================================== epoch_id: {epoch_id} start ======================================')
    df.persist()
    print(f'streaming dataframe show()')

    df.show(truncate=False)

    json_to_col_df = df.select(
        from_json(col('VALUE'), schema).alias('person_info')
    ).select('person_info.*') \
     .selectExpr('name',
                 'address.*',
                 'CAST(age AS INT) AS age',
                 'hobbies')

    print(f'json_to_col_df.show()')
    json_to_col_df.show(truncate=False)
    exploded_df = json_to_col_df.select('name',
                                        'country',
                                        'city',
                                        'age',
                                        explode(col('hobbies')).alias('hobbies')
    )
    print(f'exploded_df.show()')
    exploded_df.show(truncate=False)

    df.unpersist()
    print(f'====================================== epoch_id: {epoch_id} end ======================================')


app_name = 'json_with_list'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "lesson.spark-streaming.person_info2") \
                .option('startingOffsets','latest') \
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