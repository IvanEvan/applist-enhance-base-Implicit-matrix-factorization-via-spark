#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/23 10:48
# @Author  : Evan / Ethan
# @File    : 02_train_ALS_MF_spark.py
import sys
import logging

from pyspark.sql import HiveContext, SparkSession
from pyspark.mllib.recommendation import Rating, ALS, MatrixFactorizationModel

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.INFO)


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    spark_appname = 'user-applist-matirx'
    in_table = 'temp.evany_udid_applist_intid'
    # all_out_table = 'temp.evany_udid_appid_score'
    user_out_table = 'temp.evany_udid_embedding'
    item_out_table = 'temp.evany_app_embedding'

    spark = SparkSession \
        .builder \
        .appName(spark_appname) \
        .enableHiveSupport() \
        .getOrCreate()
    logging.info('===== spark running... =====')

    sc = spark.sparkContext
    hive_context = HiveContext(sc)

    # 取出 用户去重排序后索引 app去重排序后索引 安装标识1 共三列
    df_data = hive_context.sql("select *, 1 as rate from %s" % in_table)
    rdd_data = df_data.rdd

    # ALS 仅接收int类型数据
    total_rates_data = rdd_data.map(lambda x: Rating(int(x[0]), int(x[1]), int(x[2])))
    logging.info('===== all training data of %s is stand by =====' % str(total_rates_data.count()))

    # 随机抽取几条数据
    # sub_rates_data = total_rates_data.takeSample(withReplacement=False, num=10, seed=93)
    # rates_data = sc.parallelize(sub_rates_data)  # list to rdd
    # logging.info(type(rates_data))
    # logging.info('===== sub sample training data of %s is stand by =====' % str(rates_data.count()))
    rates_data = total_rates_data

    # 采用隐式ALS
    model = ALS.trainImplicit(ratings=rates_data, rank=200, iterations=10, lambda_=0.01, alpha=40.0)
    # model.save(sc, 'implicit.model')
    logging.info('===== training done =====')

    # test_data = rates_data.map(lambda p: (p[0], p[1]))
    # # 预测所有已安装的app
    # predict_result = model.predictAll(test_data)
    # # 预测 所有用户 个人已安装的app
    # out_rdd = predict_result.map(lambda r: (r[0], r[1], r[2]))
    # # rdd 转 dataframe
    # user_embd_df = spark.createDataFrame(out_rdd, ['udid_unique', 'appid_unique', 'score'])
    # # 将 DataFrame 数据转成 table：registerDataFrameAsTable
    # hive_context.registerDataFrameAsTable(user_embd_df, tableName='testhive_all')  # 生成虚拟表，设置表名
    # # 在上一个sql文件中完成创建空表 在这一步插入值
    # hive_context.sql("insert overwrite table %s select * from testhive_all" % all_out_table)
    # logging.info("===== output user's app score =====")

    # 所有用户的身份编码
    user_embedding = model.userFeatures()
    user_embed_rdd = user_embedding.map(lambda x: (x[0], str(x[1].tolist()).strip(']').strip('[').replace(' ', '')))
    user_embed_df = spark.createDataFrame(user_embed_rdd, ['udid_unique', 'embed_vector'])
    # 将 DataFrame 数据转成 table：registerDataFrameAsTable
    hive_context.registerDataFrameAsTable(user_embed_df, tableName='testhive_user')  # 生成虚拟表，设置表名
    # 在上一个sql文件中完成创建空表 在这一步插入值
    hive_context.sql("insert overwrite table %s select * from testhive_user" % user_out_table)
    logging.info("===== output user's embedding done =====")

    # 所有商品的编码
    item_embedding = model.productFeatures()
    item_embed_rdd = item_embedding.map(lambda x: (x[0], str(x[1].tolist()).strip(']').strip('[').replace(' ', '')))
    item_embed_df = spark.createDataFrame(item_embed_rdd, ['appid_unique', 'embed_vector'])
    # 将 DataFrame 数据转成 table：registerDataFrameAsTable
    hive_context.registerDataFrameAsTable(item_embed_df, tableName='testhive_app')  # 生成虚拟表，设置表名
    # 在上一个sql文件中完成创建空表 在这一步插入值
    hive_context.sql("insert overwrite table %s select * from testhive_app" % item_out_table)
    logging.info("===== output item's embedding done =====")

    # Evaluate the model on training data
    # predictions = predict_result.map(lambda r: ((r[0], r[1]), r[2]))
    # ratesAndPreds = rates_data.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    # MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
    # logging.info('===== Mean Squared Error : %s =====' % str(MSE))
    # logging.info('===== num of predict result: %s =====' % str(predictions.count()))

    # 计算量太大 暂时弃用 80w+用户 35w+app
    # 预测 所有用户 的 所有app 的分数
    # model = MatrixFactorizationModel.load(sc, 'implicit.model')
    # all_user_rdd = rates_data.map(lambda x: int(x[0])).distinct()
    # all_product_rdd = rates_data.map(lambda x: int(x[1])).distinct()
    #
    # user_number = all_user_rdd.count()  # 所有用户数
    # part_number = 50
    # if user_number % part_number:  # 有余数
    #     loop_num = int(user_number / part_number) + 1
    # else:
    #     loop_num = int(user_number / part_number)
    #
    # for i in range(loop_num):
    #     if i != loop_num-1:
    #         sub_all_user_rdd = all_user_rdd.filter(lambda x: part_number*i <= x < part_number*(i+1))
    #     else:
    #         sub_all_user_rdd = all_user_rdd.filter(lambda x: part_number * i <= x)
    #
    #     # spark 笛卡尔积 大表在前 小表在后
    #     descartes_rdd = all_product_rdd.cartesian(sub_all_user_rdd).map(lambda r: (r[1], r[0]))
    #     out_rdd = model.predictAll(descartes_rdd).map(lambda r: (r[0], r[1], r[2]))
    #
    #     # rdd 转 dataframe
    #     user_embd_df = spark.createDataFrame(out_rdd, ['userid_unique', 'appid_unique', 'score'])
    #
    #     # 将 DataFrame 数据转成 table：registerDataFrameAsTable
    #     hive_context.registerDataFrameAsTable(user_embd_df, tableName='testhive')  # 生成虚拟表，设置表名
    #
    #     # 在上一个sql文件中完成创建空表 在这一步插入值
    #     hive_context.sql('insert into table temp.evany_userid_appid_score select * from testhive')
    #     logging.info("===== output user's app score =====")
