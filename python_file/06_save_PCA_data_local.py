#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/13 17:16
# @Author  : Evan / Ethan
# @File    : 05_save_PCA_data_local.py
import logging
from pyspark.sql import HiveContext, SparkSession

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.INFO)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('catch-PCA-data') \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    hive_context = HiveContext(sc)

    sdf = spark.sql('select client_uid,pcafeaturesx,pcafeaturesy from temp.evany_userid_embedding_PCA_vector') \
        .repartition(100)
    logging.info('===== load PCA data done =====')

    sdf \
        .repartition(100) \
        .write \
        .format('csv') \
        .option("header", "false") \
        .mode('overwrite') \
        .save('file:///~/applist_enhance/cache_file/pca_vec')
    logging.info('===== save to csv file done =====')

