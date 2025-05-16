from common.ch13_4.base_stream_app import BaseStreamApp
from pyspark.sql.functions import pandas_udf, expr, floor
from pyspark.sql.types import IntegerType
from datetime import datetime
import pandas as pd
import time


class PandasUdf(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name
        self.SPARK_EXECUTOR_INSTANCES = '1'
        self.SPARK_EXECUTOR_MEMORY = '3g'
        self.SPARK_EXECUTOR_CORES = '1'

    def main(self):
        self.logger.write_log('info','Job begin')
        spark = self.get_session_builder() \
            .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
            .getOrCreate()
        birth_df = spark.range(10000000).toDF("id")  # 임의의 ID 컬럼 생성
        birth_df = birth_df \
            .withColumn(
                'age',
                floor(10 + (60 - 10 + 1) * expr('rand()'))
            ).withColumn(
                'BIRTH_YEAR_DECORATOR',
                PandasUdf.get_birth('age')
            )
        birth_df.groupBy('BIRTH_YEAR_DECORATOR').count().show(100)

        self.logger.write_log('info','Job end')
        time.sleep(600)


    @staticmethod
    @pandas_udf(returnType=IntegerType())
    def get_birth(value: pd.Series) -> pd.Series:
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year


if __name__ == '__main__':
    pandas_udf = PandasUdf(app_name='pandas_udf')
    pandas_udf.main()