from common.ch13_4.base_stream_app import BaseStreamApp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession


class RtBicycleRent(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        # 체크포인트 경로 설정, sparkSession 변수를 통해 설정합니다.
        spark.sparkContext.setCheckpointDir(self.dataframe_chkpnt_dir)

        # rslt_df 데이터프레임 공유하기
        self.rslt_df = spark.createDataFrame([],'STT_ID STRING, BASE_DT STRING, RENT_CNT INT, RETURN_CNT INT')

        streaming_query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
            .option("subscribe", "lesson.spark-streaming.rslt-sample") \
            .option('startingOffsets', 'latest') \
            .option('failOnDataLoss', 'false') \
            .load() \
            .selectExpr(
                "CAST(key AS STRING) AS KEY",
                "CAST(value AS STRING) AS VALUE"
             ) \
            .select(
                 get_json_object(col('VALUE'),'$.STT_ID').alias('STT_ID')
                ,get_json_object(col('VALUE'),'$.BASE_DT').alias('BASE_DT')
                ,get_json_object(col('VALUE'), '$.RENT_CNT').cast(IntegerType()).alias('RENT_CNT')
                ,get_json_object(col('VALUE'), '$.RETURN_CNT').cast(IntegerType()).alias('RETURN_CNT')
             ) \
            .writeStream \
            .foreachBatch(lambda df, epoch_id: self.for_each_batch(df, epoch_id, spark)) \
            .option("checkpointLocation", self.kafka_offset_dir) \
            .start()
        streaming_query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id, spark:SparkSession):
        self.rslt_df = self.rslt_df.alias('r').join(
            other   = df.alias('i'),
            on      = ['STT_ID','BASE_DT'],
            how     = 'full'
        ).selectExpr(
            'CASE WHEN r.STT_ID IS NULL THEN i.STT_ID ELSE r.STT_ID END         AS STT_ID',
            'CASE WHEN r.BASE_DT IS NULL THEN i.BASE_DT ELSE r.BASE_DT END      AS BASE_DT',
            'NVL(r.RENT_CNT,0) + NVL(i.RENT_CNT,0)                              AS RENT_CNT',
            'NVL(r.RETURN_CNT,0) + NVL(i.RETURN_CNT,0)                          AS RETURN_CNT'
        ).filter(col('STT_ID').isNotNull() | col('BASE_DT').isNotNull())

        self.logger.write_log('info','rslt_df.show()',epoch_id)
        before_rdd_id = self.rslt_df.rdd.id()
        self.rslt_df = self.rslt_df.checkpoint()
        after_rdd_id = self.rslt_df.rdd.id()

        # checkpoint 과정에서 Spark 내부적으로 여러 단계로 처리되며 rdd-id 가 증가하게 됩니다.
        if self.log_mode == 'debug':
            self.logger.write_log('debug', f'self.rslt_df 체크포인트 완료, rdd_id 범위:{before_rdd_id} ~ {after_rdd_id})', epoch_id)
            self.logger.write_log('debug', f'self.rslt_df.explain()', epoch_id)
            self.rslt_df.explain()
        self.rslt_df.show(truncate=False)


if __name__ == '__main__':
    rt_bicycle_rent = RtBicycleRent(app_name='dataframe_checkpoint')
    rt_bicycle_rent.main()