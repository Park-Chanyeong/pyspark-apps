from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession    \
        .builder    \
        .appName('dataframe_cached')    \
        .getOrCreate()

print(f'spark application start')

company_emp_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'
company_emp_schema = 'company_id LONG,employee_count LONG,follower_count LONG,time_recorded TIMESTAMP'
company_ind_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
company_ind_schema = 'company_id LONG, industry STRING'

company_emp_df = spark.read \
                .option('header', 'true') \
                .option('multiLine', 'true') \
                .schema(company_emp_schema) \
                .csv(company_emp_path)

company_emp_df.persist()
emp_cnt = company_emp_df.count()
print(f'company_emp_df.count : {emp_cnt}')

company_emp_dedup_df = company_emp_df.dropDuplicates(['company_id'])
emp_dedup_dnt = company_emp_dedup_df.count()
print(f'company_emp_dedup_df.count : {emp_dedup_dnt}')

company_idu_df = spark.read \
                .option('header', 'true') \
                .option('multiLine', 'true') \
                .schema(company_ind_schema) \
                .csv(company_ind_path)
company_idu_df.persist()
idu_cnt = company_idu_df.count()
print(f'company_idu_df.count : {idu_cnt}')

company_it_df = company_idu_df.filter(col('industry') == 'IT Services and IT Consulting')

company_emp_cnt_df = company_emp_dedup_df.join(
    other=company_it_df,
    on='company_id',
    how='inner',
).select('company_id', 'employee_count') \
    .sort('employee_count', ascending=False)

company_emp_cnt_df.show()
time.sleep(300)