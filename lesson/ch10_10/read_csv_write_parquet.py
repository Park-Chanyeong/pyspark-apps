from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName('read_csv_write_parquet') \
    .getOrCreate()

path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/companies.csv'
schema = 'company_id   STRING,' \
         'name         STRING,' \
         'description  STRING,' \
         'company_size INT,' \
         'state        STRING,' \
         'country      STRING,' \
         'city         STRING,' \
         'zip_code     STRING,' \
         'address      STRING,' \
         'url          STRING'

csv_df = spark \
        .read \
        .option("header", 'true') \
        .option('multiLine','true') \
        .schema(schema) \
        .csv(path)
csv_df.persist()
print('Complete: Read companies.csv')
csv_df.show()

csv_df.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('hdfs:///home/spark/lesson/parquet/companies')

# 아래와 같이 작성해도 동일하게 동작
# csv_df.write.mode('overwrite').parquet('hdfs:///home/spark/lesson/parquet/companies')

print('Complete: Save companies as parquet')

