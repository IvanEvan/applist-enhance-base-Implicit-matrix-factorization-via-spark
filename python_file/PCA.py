#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/2 17:15
# @Author  : Evan / Ethan
# @File    : PCA.py
import array
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.mllib.linalg import SparseVector, VectorUDT
from pyspark.sql.types import StringType, StructField, StructType


def str_2_list(str_vec):
    return array.array('d', eval('[' + str_vec + ']'))


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName('user-embedding-PCA')\
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse')\
        .enableHiveSupport()\
        .getOrCreate()

    sdf = spark.sql('select embed_vector from temp.xxx').repartition(100)
    # rdd_data = sdf.rdd

    # list_rdd_data = rdd_data.map(lambda x: (eval('[' + str(x) + ']')))

    # df = spark.createDataFrame(list_rdd_data, ["features"])

    df = sdf.withColumn('features', str_2_list('embed_vector'))
    # df = spark.createDataFrame(df, StructType([StructField("features", VectorUDT(), True)]))

    pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)

    result = model.transform(df).select("pcaFeatures")
    result.show(truncate=False)

