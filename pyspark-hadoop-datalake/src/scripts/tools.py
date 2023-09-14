import findspark
findspark.init()
findspark.find()
import os
import pyspark
from pyspark.sql import SparkSession


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

def spark_session_init(name):
    return SparkSession \
        .builder \
        .master("yarn")\
        .appName(f"{name}") \
        .getOrCreate()


def write_df(df, df_name, date):
    df.write.mode('overwrite').parquet(f'/user/konstantin/prod/{df_name}/date={date}')


