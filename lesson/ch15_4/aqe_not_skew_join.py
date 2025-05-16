from common.ch13_4.base_stream_app import BaseStreamApp
from pyspark.sql.functions import when
import time


class AqeNotSkewJoin(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name
        self.SPARK_EXECUTOR_INSTANCES = '2'
        self.SPARK_EXECUTOR_MEMORY = '3g'
        self.SPARK_EXECUTOR_CORES = '2'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '10'

        # 개별 지정하는 옵션
        # Skew join이 적용되지 않은 Sort Merge Join 으로 유도
        self.SPARK_SQL_ADAPTIVE_SKEWJOIN_SKEWEDPARTITIONTHRESHOLDINBYTES = '5M'
        self.SPARK_SQL_ADAPTIVE_ADVISORYPARTITIONSIZEINBYTES = '3M'
        self.SPARK_SQL_ADAPTIVE_COALESCEPARTITIONS_ENABLED = 'false'
        self.SPARK_SQL_ADAPTIVE_SKEWJOIN_ENABLED = 'false'
        self.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD = '-1'

    def main(self):
        spark = self.get_session_builder() \
            .config('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes', self.SPARK_SQL_ADAPTIVE_SKEWJOIN_SKEWEDPARTITIONTHRESHOLDINBYTES) \
            .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', self.SPARK_SQL_ADAPTIVE_ADVISORYPARTITIONSIZEINBYTES) \
            .config('spark.sql.adaptive.coalescePartitions.enabled', self.SPARK_SQL_ADAPTIVE_COALESCEPARTITIONS_ENABLED) \
            .config('spark.sql.adaptive.skewJoin.enabled', self.SPARK_SQL_ADAPTIVE_SKEWJOIN_ENABLED) \
            .config("spark.sql.autoBroadcastJoinThreshold", self.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD) \
            .getOrCreate()
        self.logger.write_log('info','Job begin')

        skewed_df = spark.createDataFrame([(i, f"value-{i}") for i in range(0, 1500000)],'num LONG, data STRING')
        skewed_df = skewed_df.withColumn(
            'id',
            when((skewed_df.num >= 0) & (skewed_df.num < 10000), 0) \
                .when((skewed_df.num >= 10000) & (skewed_df.num < 20000), 1) \
                .when((skewed_df.num >= 20000) & (skewed_df.num < 30000), 2) \
                .when((skewed_df.num >= 30000) & (skewed_df.num < 40000), 3) \
                .when((skewed_df.num >= 40000) & (skewed_df.num < 50000), 4) \
                .when((skewed_df.num >= 50000) & (skewed_df.num < 60000), 5) \
                .when((skewed_df.num >= 60000) & (skewed_df.num < 70000), 6) \
                .when((skewed_df.num >= 70000) & (skewed_df.num < 80000), 7) \
                .when((skewed_df.num >= 80000) & (skewed_df.num < 90000), 8) \
                .otherwise(9)
        )

        join_data = [(i, f"join-value-{i}") for i in range(0, 10)]
        join_df = spark.createDataFrame(join_data, ["id", "join_value"])
        result_df = skewed_df.join(join_df, on='id', how="inner")
        result_df.collect()

        self.logger.write_log('info','Job end')
        time.sleep(1200)

if __name__ == '__main__':
    aqe_not_skew_join = AqeNotSkewJoin(app_name='aqe_not_skew_join')
    aqe_not_skew_join.main()