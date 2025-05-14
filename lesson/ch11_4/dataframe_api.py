from pyspark.sql.functions import col, lit, count, upper, broadcast
from pyspark.sql import SparkSession
import time
spark = SparkSession \
    .builder \
    .appName('dataframe_api') \
    .getOrCreate()

postings_path = 'hdfs:///home/spark/lesson/parquet/postings'
job_skill_path = 'hdfs:///home/spark/lesson/parquet/job_skills'
postings_df = spark.read.parquet(postings_path)
job_skill_df = spark.read.parquet(job_skill_path)
postings_df = postings_df.filter(upper(col('work_type')).isin(['FULL-TIME','FULL_TIME']))

rslt_df = postings_df.join(
        other=broadcast(job_skill_df),
        on='job_id',
        how='inner'
    ).groupBy('skill_abr').agg(
         count('job_id').alias('cnt_job_id')
    ).sort('cnt_job_id', ascending=False)
rslt_df.explain()
rslt_df.show()
time.sleep(1200)