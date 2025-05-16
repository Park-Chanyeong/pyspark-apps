from common.ch13_1.base_stream_app import BaseStreamApp
from pyspark.sql.functions import broadcast


class IncorrectBroadcast(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_EXECUTOR_INSTANCES = '1'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_EXECUTOR_CORES = '1'

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()
        self.logger.write_log('info','Job begin')

        large_df = spark.createDataFrame([(i, f"value-{i}") for i in range(0, 5000000)],'num LONG, data STRING')
        small_df = spark.createDataFrame([(i, f"value-{i}") for i in range(0, 10)],'num LONG, data STRING')

        rslt_df = small_df.join(broadcast(large_df),on='num', how='inner')
        rslt_df.show()
        self.logger.write_log('info','Job end')

if __name__ == '__main__':
    incorrect_broadcast = IncorrectBroadcast(app_name='incorrect_broadcast')
    incorrect_broadcast.main()