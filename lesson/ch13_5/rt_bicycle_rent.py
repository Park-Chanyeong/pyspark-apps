from common.ch13_4.base_stream_app import BaseStreamApp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from datetime import datetime

class RtBicycleRent(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_EXECUTOR_INSTANCES = '3'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_EXECUTOR_CORES = '2'
        self.last_dttm = ''

    def init_call(self, spark_session: SparkSession):
        '''
        Spark 프로그램 기동할 때마다 1회만 수행되는 함수
        '''
        self.last_stt_info_df = spark_session.createDataFrame([],
                                                     'stt_id          STRING,'
                                                     'lst_prk_cnt     INT'
                                                     )

        # 결과 저장용 테이블
        spark_session.sql(f'''
            CREATE TABLE IF NOT EXISTS lesson.bicycle_rent_info(
                stt_id          STRING,
                stt_nm          STRING,
                rent_cnt        INT,
                return_cnt      INT,
                lst_prk_cnt     INT,
                stt_lttd        STRING,
                stt_lgtd        STRING,
                crt_dttm        TIMESTAMP
            )
            LOCATION 's3a://datalake-spark-sink-pcy/lesson/bicycle_rent_info'
            PARTITIONED BY (ymd STRING, hh STRING)
            STORED AS PARQUET
              '''
        )
        self.logger.write_log('info', 'Completed: CREATE TABLE IF NOT EXISTS lesson.bicycle_rent_info')

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        # 체크포인트 경로 설정, sparkSession 변수를 통해 설정합니다.
        spark.sparkContext.setCheckpointDir(self.dataframe_chkpnt_dir)

        self.init_call(spark)

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
            .option("checkpointLocation", self.kafka_offset_dir) \
            .start()
        streaming_query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id: int, spark: SparkSession):
        '''
        bicycle-producer 에서 1회 전송하는 건수는 약 3400 건이나, Micro Batch 시작시 df 건수는 3400 미만이 되거나
        maxOffsetsPerTrigger(=10000) 정도의 건수가 될 수 있으므로(Spark job Down 후 재시작시) 이를 고려하여 함수 작성 필요
        '''
        df.persist()
        self.logger.write_log('info', f'df.count(): {df.count()}', epoch_id)

        distinct_dttm = sorted([row['crt_dttm'] for row in df.select('crt_dttm').distinct().collect()])
        self.logger.write_log('info', f'stream DataFrame 내 dttm 카운트: {len(distinct_dttm)}', epoch_id)

        for dttm in distinct_dttm:
            self.sink_to_s3(
                last_stt_df=self.last_stt_info_df,
                dttm=dttm,
                stream_df=df.filter(col('crt_dttm') == dttm),
                epoch_id=epoch_id
            )

        df.unpersist()

    def sink_to_s3(self, last_stt_df: DataFrame, dttm: str, stream_df: DataFrame, epoch_id):
        if self.log_mode == 'debug':
            self.logger.write_log('debug', 'stream_df.show()', epoch_id)
            stream_df.show(truncate=False)

        cloned_last_stt_df = last_stt_df.select('*')

        # 마지막 처리한 dttm과 현재 처리 중인 dttm 시간 구간이 10분 이상 차이나면 (bicycle-producer 중단 후 재기동한 경우)
        # rent_cnt, return_cnt 에 왜곡이 생기므로 결과 전송하지 않고 last_stt_df 를 빈 데이터프레임으로 만들어 self.last_stt_info_df 만 갱신하도록 유도
        if len(self.last_dttm) > 0:
            time_diff = datetime.strptime(dttm, '%Y-%m-%d %H:%M:%S') - datetime.strptime(self.last_dttm, '%Y-%m-%d %H:%M:%S')
            if time_diff.days >= 0 and time_diff.seconds > 600:     # 10분
                cloned_last_stt_df = cloned_last_stt_df.filter(col('stt_id') == 'nothing')

        # 마지막 정류소별 보관 대수를 저장한 last_stt_df 를 이용해 이번 시간대 대여소별 따릉이 RENT, RETURN 개수를 구합니다.
        # 만약 last_stt_df 의 lst_prk_cnt 컬럼이 null일 경우 diff_prk_cnt 값은 0 으로 셋팅합니다.
        processed_df = stream_df.alias('s').join(
            other=cloned_last_stt_df.alias('l'),
            on='stt_id',
            how='left'
        ).selectExpr(
            'stt_id',
            'TO_TIMESTAMP(s.crt_dttm)                                           AS crt_dttm',
            's.stt_nm                                                           AS stt_nm',
            'NVL(l.lst_prk_cnt - tot_prk_cnt, 0)                                AS diff_prk_cnt',
            's.tot_prk_cnt                                                      AS tot_prk_cnt',
            's.stt_lttd                                                         AS stt_lttd',
            's.stt_lgtd                                                         AS stt_lgtd',
        ).selectExpr(
            'stt_id                                                       AS stt_id',
            "DATE_FORMAT(crt_dttm,'yyyyMMdd')                                   AS ymd",
            "DATE_FORMAT(crt_dttm,'HH')                                         AS hh",
            'STT_NM                                                             AS stt_nm',
            f'''
                CASE WHEN diff_prk_cnt > 0 THEN   diff_prk_cnt 
                     ELSE 0
                END                                                             AS rent_cnt''',
            f'''
                CASE WHEN diff_prk_cnt < 0 THEN   -1 * diff_prk_cnt
                     ELSE 0
                END                                                             AS return_cnt''',
            'tot_prk_cnt                                                        AS lst_prk_cnt',
            'stt_lttd                                                           AS stt_lttd',
            'stt_lgtd                                                           AS stt_lgtd',
            'crt_dttm                                                           AS crt_dttm'
        ).filter(
            col('stt_id').isNotNull()
        ).persist()

        if self.log_mode == 'debug':
            self.logger.write_log('debug', 'processed_df.show()', epoch_id)
            processed_df.show(truncate=False)

        # spark job 최초 기동시 last_stt_info_df 데이터프레임은 비어있으므로 RENT_CNT와 RETURN_CNT 값은 부정확한 상태
        # 이 경우 processed_df 는 전송하지 않고 LST_PRK_CNT 값을 이용해 last_stt_info_df를 만드는데만 사용
        # 다음 데이터부터 전송하기 시작
        # processed_df 파티션 개수를 1개로 축소한 후 S3에 전송 (너무 많은 파일이 생기지 않도록)
        if not cloned_last_stt_df.isEmpty():
            # Sink to S3
            processed_df.coalesce(1).write \
                .format("parquet") \
                .mode('append') \
                .option("path", "s3a://datalake-spark-sink-pcy/lesson/bicycle_rent_info") \
                .partitionBy("ymd", "hh") \
                .save()

            self.logger.write_log('info', f'Completed: Sink to S3 (기준 시간: {dttm})', epoch_id)

        now_stt_info_df = cloned_last_stt_df.alias('l').join(
                other=processed_df.alias('p'),
                on=['stt_id'],
                how='full'
        ).selectExpr(
            'stt_id                                               AS stt_id',
            'NVL(p.lst_prk_cnt, l.lst_prk_cnt)                      AS lst_prk_cnt'
        ).checkpoint()
        self.logger.write_log('info', f'Completed: Checkpoint(last stt info dataframe)', epoch_id)

        if self.last_stt_info_df.is_cached: self.last_stt_info_df.unpersist()
        self.last_stt_info_df = now_stt_info_df.persist()
        self.last_dttm = dttm       # 마지막 처리한 dttm 시간 정보 저장

        processed_df.unpersist()


if __name__ == '__main__':
    rt_bicycle_rent = RtBicycleRent(app_name='rt_bicycle_rent')
    rt_bicycle_rent.main()
