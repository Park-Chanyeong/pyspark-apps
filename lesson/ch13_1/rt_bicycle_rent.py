from common.ch13_1.base_stream_app import BaseStreamApp
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class RtBicycleRent(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_EXECUTOR_INSTANCES = '2'
        self.SPARK_EXECUTOR_CORES = '2'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '4'

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        streaming_query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
            .option("subscribe", "apis.seouldata.rt-bicycle") \
            .option('startingOffsets','latest') \
            .option('failOnDataLoss', 'false') \
            .load() \
            .selectExpr(
                "CAST(key AS STRING) AS KEY",
                "CAST(value AS STRING) AS VALUE"
             ) \
            .writeStream \
            .foreachBatch(lambda df, epoch_id: self.for_each_batch(df, epoch_id, spark)) \
            .option("checkpointLocation", self.kafka_offset_dir) \
            .start()
        streaming_query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        부모 클래스 함수 오버라이딩, 본 클래스의 로직 작성
        '''
        df.persist()
        cnt = df.count()
        self.logger.write_log('info', f'streaming 인입 건수: {cnt}')
        self.logger.write_log('info', f'df.show()')
        df.show(truncate=False)
        df.unpersist()


if __name__ == '__main__':
    rt_bicycle_rent = RtBicycleRent(app_name='rt_bicycle_rent')
    rt_bicycle_rent.main()