from pyspark.sql import SparkSession
import time


spark = SparkSession \
    .builder \
    .appName('spark_sql') \
    .getOrCreate()

postings_path = 'hdfs:///home/spark/lesson/parquet/postings'
job_skill_path = 'hdfs:///home/spark/lesson/parquet/job_skills'
postings_df = spark.read.parquet(postings_path)
job_skill_df = spark.read.parquet(job_skill_path)
postings_df.createOrReplaceTempView('postings')
job_skill_df.createOrReplaceTempView('job_skills')

sql = f'''
SELECT /*+ BROADCAST(j) */
j.skill_abr, count(p.job_id) as cnt_job_id
FROM postings p
JOIN job_skills j
    ON p.job_id = j.job_id 
WHERE UPPER(p.work_type) IN ('FULL_TIME','FULL-TIME')
GROUP BY j.skill_abr 
ORDER BY count(p.job_id) DESC
'''
rslt_df = spark.sql(sql)
rslt_df.explain()
rslt_df.show()
time.sleep(1200)