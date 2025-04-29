from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import time

spark = SparkSession \
        .builder \
        .appName('driver_and_executor') \
        .config('spark.executor.memory', '2g') \
        .config('spark.executor.instances', '3') \
        .config('spark.executor.cores', '2') \
        .getOrCreate()

print(f'spark application start')
postings_path = 'hdfs:///home/spark/sample/linkedin_jobs/postings.csv'
postings_schema = 'job_id                     LONG, ' \
                  'company_name               STRING, ' \
                  'title                      STRING, ' \
                  'description                STRING, ' \
                  'max_salary                 LONG, ' \
                  'pay_period                 STRING, ' \
                  'location                   STRING, ' \
                  'company_id                 LONG, ' \
                  'views                      LONG, ' \
                  'med_salary                 LONG, ' \
                  'min_salary                 LONG, ' \
                  'formatted_work_type        STRING, ' \
                  'applies                    LONG, ' \
                  'original_listed_time       TIMESTAMP, ' \
                  'remote_allowed             STRING, ' \
                  'job_posting_url            STRING,' \
                  'application_url            STRING, ' \
                  'application_type           STRING, ' \
                  'expiry                     STRING, ' \
                  'closed_time                TIMESTAMP, ' \
                  'formatted_experience_level STRING, ' \
                  'skills_desc                STRING, ' \
                  'listed_time                TIMESTAMP, ' \
                  'posting_domain             STRING, ' \
                  'sponsored                  LONG, ' \
                  'work_type                  STRING, ' \
                  'currency                   STRING, ' \
                  'compensation_type          STRING'

# postings Load
postings_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(postings_schema) \
                 .csv(postings_path)
postings_df = postings_df.repartition(6,'job_id').persist()
postings_cnt = postings_df.count()
print(f'postings_df count: {postings_cnt}')

time.sleep(300)