from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from common.logger import Logger
from confluent_kafka import Consumer, TopicPartition
import argparse
import os, json


class BaseStreamApp():
    def __init__(self, app_name):
        self.app_name = app_name
        self.kafka_offset_dir = f'/home/spark/kafka_offsets/{app_name}'  # Kafka Offset Checkpoint 경로 지정
        self.dataframe_chkpnt_dir = f'/home/spark/dataframe_checkpoints/{self.app_name}'  # DataFrame Checkpoint 경로 지정

        # Spark Parameter 에 대한 설정
        # 잘 변경되지 않으며 고정되는 파라미터는 $SPARK_HOME/conf/spark-defaults.conf 에 설정하고
        # 프로그램마다 변경될 수 있는 파라미터들은 이 함수에 정의하도록 합니다.

        self.SPARK_EXECUTOR_INSTANCES = '3'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_EXECUTOR_CORES = '2'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '6'

        # 파라미터 input 설정
        self.log_mode = ''
        self.set_argparse()
        self.logger = Logger(app_name)

        # Kafka 관련 파라미터 셋팅
        self.KAFKA_BOOTSTRAP_SERVERS = 'kafka01:9092,kafka02:9092,kafka03:9092'
        self.kafka_group_id = f'SPARK_STREAMING_{app_name}'
        self.consumer = Consumer(
            {
                'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVERS,
                'group.id': self.kafka_group_id,
                'enable.auto.commit':'false'
            }
        )

    def set_argparse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-l", "--log_mode", default='info', help='info or debug')
        args = parser.parse_args()
        self.log_mode = args.log_mode

    def get_session_builder(self):
        return SparkSession \
            .builder \
            .appName(self.app_name) \
            .config('spark.executor.memory', self.SPARK_EXECUTOR_MEMORY) \
            .config('spark.executor.instances', self.SPARK_EXECUTOR_INSTANCES) \
            .config('spark.executor.cores', self.SPARK_EXECUTOR_CORES) \
            .config('spark.sql.shuffle.partitions', self.SPARK_SQL_SHUFFLE_PARTITIONS)

    def for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        _for_each_batch 함수 실행 전, 후 공통 로직 삽입용도
        '''
        self.logger.write_log('info',f'============================= epoch_id: {epoch_id} start =============================',epoch_id)
        self._for_each_batch(df, epoch_id, spark)
        self.offset_commit(epoch_id)
        self.logger.write_log('info',f'============================= epoch_id: {epoch_id} end =============================',epoch_id)

    def _for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        Spark Streaming Application 중 for each batch를 사용하는 경우 해당 함수를 재정의하여 사용합니다.
        '''
        pass

    def offset_commit(self, epoch_id):
        offset_info = self.get_popen_rslt(f'/engine/hadoop-3.3.6/bin/hdfs dfs -cat /home/spark/kafka_offsets/{self.app_name}/offsets/{epoch_id}')

        offset_info_dict = json.loads(offset_info.split('\n')[2])  # 첫 번째 라인은 v1, 두 번째 라인은 옵션list, 세 번째 라인이 offset 정보
        offset_target_lst = []
        for topic_name, offset_info in offset_info_dict.items():
            for ptt, offset in offset_info.items():
                offset_target_lst.append(
                    TopicPartition(
                        topic=topic_name,
                        partition=int(ptt),
                        offset=offset
                    )
                )
        self.consumer.commit(offsets=offset_target_lst)
        self.logger.write_log('info',f'{self.kafka_group_id} commit 완료 ({offset_target_lst})', epoch_id)

    def get_popen_rslt(self, cmd):
        return os.popen(cmd).read()