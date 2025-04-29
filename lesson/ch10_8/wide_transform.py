from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, count
import time

spark = SparkSession \
        .builder \
        .appName('wide_transform.py') \
        .config('spark.executor.memory', '2g') \
        .config('spark.executor.instances', '3') \
        .config('spark.executor.cores', '2') \
        .config('spark.sql.adaptive.enabled', 'false') \
        .getOrCreate()

print(f'spark application start')
job_skills_path = 'hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv'
job_skills_schema = 'job_id LONG, skill_abr STRING'
skills_path = 'hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv'
skills_schema = 'skill_abr STRING, skill_name STRING'

# job_skills load
job_skills_df = spark.read \
                 .option('header','true') \
                 .schema(job_skills_schema) \
                 .csv(job_skills_path)

print(f'job_skills load 완료')

# skills load
skills_df = spark.read \
                 .option('header','true') \
                 .schema(skills_schema) \
                 .csv(skills_path)

print(f'skills load 완료')

cnt_per_skills_df = job_skills_df.join(
    other=broadcast(skills_df),
    on='skill_abr',
    how='inner'
).select('job_id', 'skill_name') \
    .groupBy('skill_name') \
    .agg(count('job_id').alias('job_count')) \
    .sort('job_count', ascending=False)

print(cnt_per_skills_df.count())
time.sleep(1200)