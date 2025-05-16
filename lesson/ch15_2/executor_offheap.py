from common.ch13_4.base_stream_app import BaseStreamApp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time


class ExecutorOffheap(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_EXECUTOR_INSTANCES = '2'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_EXECUTOR_CORES = '2'
        self.SPARK_MEMORY_OFFHEAP_SIZE = '1g'
        self.SPARK_MEMORY_OFFHEAP_ENABLED = 'true'

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder() \
                .config('spark.memory.offHeap.size',self.SPARK_MEMORY_OFFHEAP_SIZE) \
                .config('spark.memory.offHeap.enabled',self.SPARK_MEMORY_OFFHEAP_ENABLED) \
                .getOrCreate()

        postings_df = spark.table('linkedin.postings').persist(StorageLevel.OFF_HEAP)
        postings_df.show()
        time.sleep(1200)

if __name__ == '__main__':
    executor_offheap = ExecutorOffheap(app_name='executor_offheap')
    executor_offheap.main()