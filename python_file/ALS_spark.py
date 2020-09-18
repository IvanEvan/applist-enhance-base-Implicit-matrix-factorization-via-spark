#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/23 10:48
# @Author  : Evan / Ethan
# @File    : ALS_spark.py
import os


from pyspark.sql import HiveContext, SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.mllib.recommendation import Rating, ALS

from pyspark.mllib.recommendation import MatrixFactorizationModel

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession \
        .builder \
        .appName('user-applist-matirx') \
        .enableHiveSupport() \
        .getOrCreate()

sc = spark.sparkContext

hive_context = HiveContext(sc)
df_data = hive_context.sql('select *, 1 as rate from temp.evany_userid_applist_1w')
rdd_data = df_data.rdd

rates_data = rdd_data.map(lambda x: Rating(int(x[0]), int(x[1]), int(x[2])))


# sc.setCheckpointDir('checkpoint/')
# ALS.checkpointInterval = 2
model = ALS.trainImplicit(ratings=rates_data, rank=155, iterations=10, lambda_=0.01, alpha=40.0)
user_embd_rdd = model.userFeatures()

user_embd_rdd_sub = user_embd_rdd.map(lambda x: (x[0], str(x[1].tolist()).strip(']').strip('[').replace(' ', '')))

user_embd_df = spark.createDataFrame(user_embd_rdd_sub, ['userid_unique', 'embed_vector'])

print(user_embd_df.head())

# tico 用户无建表权限
# sql_str = 'create table if not exists temp.evany_userid_embedding_vector (userid_unique String, embed_vector String) stored as parquet'
# hive_context.sql(sql_str)
# 开启动态分区
# spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
# spark.sql("set hive.exec.dynamic.partition=true")
# # 指定文件格式
# user_embd_df.write.saveAsTable('temp.evany_userid_embedding_vector', format='Hive', mode='overwrite', partitionBy=None)



# 将DataFrame数据转成table：registerDataFrameAsTable
hive_context.registerDataFrameAsTable(user_embd_df, tableName='testhive') #生成虚拟表，设置表名

#若表已经建好,存在,那么使用:
hive_context.sql('insert overwrite table temp.evany_userid_embedding_vector select * from testhive')




