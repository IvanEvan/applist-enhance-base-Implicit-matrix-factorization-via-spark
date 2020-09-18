#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/2 17:15
# @Author  : Evan / Ethan
# @File    : 04_PCA_2d_vector_spark.py
import array
import logging
from pyspark.ml.feature import PCA
from pyspark.sql import HiveContext, SparkSession
from pyspark.sql.functions import udf

from pyspark.ml.linalg import Vectors, VectorUDT

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.INFO)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('user-embedding-PCA') \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    hive_context = HiveContext(sc)

    # 取出200维身份编码
    sdf = spark.sql('select client_uid,embed_vector from temp.xx') \
        .repartition(100)
    logging.info('===== load PCA data done =====')

    # 转化为适合PCA函数的向量
    rdd_data = sdf.rdd
    list_rdd_data = rdd_data.map(lambda x: (x[0], array.array('d', eval(x[1]))))
    df_sub = spark.createDataFrame(list_rdd_data, ['client_uid', 'feature_vec'])
    to_vector = udf(lambda a: Vectors.dense(a), VectorUDT())
    df = df_sub.select('client_uid', to_vector("feature_vec").alias("features"))
    logging.info('===== preprocess PCA data done =====')

    # 调用PCA算法 k=2
    pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)
    logging.info('===== PCA done =====')

    result = model.transform(df).select('client_uid', 'pcaFeatures')
    # result.show(truncate=False)

    result_rdd = result.rdd
    # 将 array 类型的向量转为string并去掉中括号
    user_embd_rdd_sub = result_rdd.map(lambda x: (x[0], x[1].tolist()[0], x[1].tolist()[1]))
    user_embd_df = spark.createDataFrame(user_embd_rdd_sub, ['client_uid', 'pcaFeaturesX', 'pcaFeaturesY'])

    # 将 DataFrame 数据转成 table：registerDataFrameAsTable
    hive_context.registerDataFrameAsTable(user_embd_df, tableName='testhive')  # 生成虚拟表，设置表名

    # 在上一个sql文件中完成创建空表 在这一步插入值
    hive_context.sql('insert overwrite table temp.evany_userid_embedding_PCA_vector select * from testhive')
    logging.info('===== output user PCA id embedding vector =====')
