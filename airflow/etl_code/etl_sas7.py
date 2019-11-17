import pandas as pd
import numpy as np
import os
import datetime

from pyspark.sql.functions import udf, col
from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

import configparser

config = configparser.ConfigParser()
config.read('../capstone.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Project: Yuli Capstone") \
        .getOrCreate()
    return spark



def get_path_sas_files(path_sas_folder):
    os.listdir(path_sas_folder)
    files_data_sas = []
    path_sas_files = [os.path.join(path_sas_folder, file_path) for file_path in os.listdir(path_sas_folder) if
                      file_path[-7:] == 'parquet']
    return path_sas_files

def convert_5_digit_sasdate_to_yyyymmdd(sasdate):
    if sasdate !=-1:
        epoch = datetime.datetime(1960, 1, 1)
        return (epoch + datetime.timedelta(days=sasdate)).strftime('%Y%m%d')
    else:
        epoch = datetime.datetime(1900, 1, 2)
        return (epoch + datetime.timedelta(days=sasdate)).strftime('%Y%m%d')

if __name__ == '__main__':
    s3_path_output = 's3a://yulicapstone/i94immi_sas_data/'
    path_sas_folder = '.././data/sas_data/'
    spark = create_spark_session()
    path_sas_files = get_path_sas_files(path_sas_folder)
    df1 = spark.read.parquet(path_sas_files[0])
    for i in range(1, len(path_sas_files)):
        df1 = df1.union(spark.read.parquet(path_sas_files[i]))

    drop_cols = ['count', 'dtadfile', 'visapost', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'biryear', 'dtaddto',
                 'insnum', 'admnum']
    df1 = df1.drop(*drop_cols)

    null_int_cols = {'cicid': -1, 'i94yr': -1, 'i94mon': -1, 'i94cit': 999, 'i94res': 239, 'i94mode': 9, 'arrdate': -1,
                     'depdate': -1, 'i94bir': -1, 'i94visa': -1}
    for k in null_int_cols:
        df1 = df1.withColumn(k, F.when((F.col(k).isNull()), null_int_cols[k]).otherwise(F.col(k).cast("int")))

    null_str_cols = {'i94port': 'XXX', 'i94addr': '99', 'visatype': 'unknown', 'occup': 'unknown', 'gender': 'U',
                     'airline': 'unknown', 'fltno': 'unknown'}
    for k in null_str_cols:
        df1 = df1.withColumn(k, F.when((F.col(k).isNull()), null_str_cols[k]).otherwise(F.col(k)))


    convert_5_digit_sasdate_to_yyyymmdd_udf = udf(lambda x: convert_5_digit_sasdate_to_yyyymmdd(x))


    df2 = df1.withColumn('arrdate', convert_5_digit_sasdate_to_yyyymmdd_udf(df1['arrdate']))
    df2 = df2.withColumn('depdate', convert_5_digit_sasdate_to_yyyymmdd_udf(df1['depdate']))

    df2 = df2.withColumn("year", df2["i94yr"])
    df2 = df2.withColumn("month", df2["i94mon"])
    df2 = df2.withColumn("date", df2["arrdate"])

    df2 = df2.dropDuplicates()
    df2.write.parquet(s3_path_output, partitionBy=["year", "month", "date"], mode='overwrite')




