from common.ch13_4.base_stream_app import BaseStreamApp
from pyspark.sql.functions import udf, expr, floor
from pyspark.sql.types import IntegerType
from datetime import datetime
import time


class BadUdf(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name
        self.SPARK_EXECUTOR_INSTANCES = '1'
        self.SPARK_EXECUTOR_MEMORY = '3g'
        self.SPARK_EXECUTOR_CORES = '1'

    def main(self):
        self.logger.write_log('info','Job begin')
        spark = self.get_session_builder().getOrCreate()
        birth_df = spark.range(10000000).toDF("id")  # 임의의 ID 컬럼 생성
        birth_df = birth_df \
            .withColumn(
                'age',
                floor(10 + (60 - 10 + 1) * expr('rand()'))
            ).withColumn(
                'BIRTH_YEAR_DECORATOR',
                BadUdf.get_birth('age')
            )

        '''
        아래처럼 udf_get_birth 라는 함수를 생성하여 적용하는 것도 가능
        udf_get_birth = udf(BadUdf.get_birth2, IntegerType())
        .withColumn(
                'BIRTH_YEAR_STT_METHOD',
                udf_get_birth('age')
            ))
        '''
        birth_df.groupBy('BIRTH_YEAR_DECORATOR').count().show(100)

        self.logger.write_log('info','Job end')
        time.sleep(600)

    # Decorator 적용 방식
    @staticmethod
    @udf(returnType=IntegerType())
    def get_birth(value):
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year

    # 함수 적용 방식
    @staticmethod
    def get_birth2(value):
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year


if __name__ == '__main__':
    bad_udf = BadUdf(app_name='bad_udf')
    bad_udf.main()