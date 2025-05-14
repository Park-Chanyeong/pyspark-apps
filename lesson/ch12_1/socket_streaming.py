from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('socket_streaming') \
    .getOrCreate()

socket_df = spark.readStream \
    .format("socket") \
    .option('host', 'localhost') \
    .option('port', '1234') \
    .load()

# query 변수는 StreamingQuery 객체이며 백그라운드에서 Active로 동작함.
query = socket_df.writeStream \
    .format('console') \
    .start()

query.awaitTermination()  # query 변수가 백그라운드에서 Active 일 때 종료되지 않도록 함