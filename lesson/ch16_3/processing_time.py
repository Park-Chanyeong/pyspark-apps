from common.ch15_8.base_stream_app import BaseStreamApp
from common.kafka_sender import send_to_kafka_json_payload
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from datetime import datetime

class ProcessingTime(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_EXECUTOR_INSTANCES = '3'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_EXECUTOR_CORES = '2'

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        # 체크포인트 경로 설정, sparkSession 변수를 통해 설정합니다.
        spark.sparkContext.setCheckpointDir(self.dataframe_chkpnt_dir)

        streaming_query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
            .option("subscribe", "apis.seouldata.rt-bicycle") \
            .option('failOnDataLoss', 'false') \
            .option('startingOffsets', 'earliest') \
            .option('maxOffsetsPerTrigger', '10000') \
            .load() \
            .selectExpr(
            "CAST(key AS STRING) AS KEY",
            "CAST(value AS STRING) AS VALUE"
        ) \
            .select(
            get_json_object(col('KEY'), '$.STT_ID').alias('stt_id')
            , get_json_object(col('KEY'), '$.CRT_DTTM').alias('crt_dttm')
            , get_json_object(col('VALUE'), '$.STT_NM').alias('stt_nm')
            , get_json_object(col('VALUE'), '$.TOT_RACK_CNT').cast(IntegerType()).alias('tot_rack_cnt')
            , get_json_object(col('VALUE'), '$.TOT_PRK_CNT').cast(IntegerType()).alias('tot_prk_cnt')
            , get_json_object(col('VALUE'), '$.STT_LTTD').alias('stt_lttd')
            , get_json_object(col('VALUE'), '$.STT_LGTD').alias('stt_lgtd')
        ) \
            .writeStream \
            .foreachBatch(lambda df, epoch: self.for_each_batch(df, epoch, spark)) \
            .trigger(processingTime='20 seconds') \
            .option("checkpointLocation", self.kafka_offset_dir) \
            .start()
        streaming_query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id: int, spark: SparkSession):
        df.persist()
        self.logger.write_log('info', f'processing_time: 20 seconds', epoch_id)
        self.logger.write_log('info', f'df.count(): {df.count()}', epoch_id)
        self.logger.write_log('info', f'df.show()', epoch_id)
        df.show()
        df.unpersist()


if __name__ == '__main__':
    processing_time = ProcessingTime(app_name='rt_bicycle_rent')
    processing_time.main()