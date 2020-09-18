#### 基于隐式矩阵因子分解的APP安装分数预测

> 根据用户applist数据，预测用户对其未安装APP和已安装APP的喜好程度

#### 原始数据为：

![raw_data](cache_file\raw_data.png)

#### 初始化两个矩阵：

![](cache_file\init_data.jpg)

#### 得到预测值：

![](cache_file\predict_data.jpg)

#### 与原始数据对比：

![](cache_file\compare.jpg)

------

#### 这其实是一种协同过滤的思想：

1. 某一个用户安装了某一类型的APP，那么他安装同类型APP的概率就会很高
2. 安装了同类APP的用户，APP列表也会更接近

#### 代码文件说明

```
|--- README.md  # 本文件
|
|--- fly.sh  # 主函数入口
|
|--- python_file
|       02_train_ALS_MF_spark.py  # 只看此文件即可，包含了采用 pyspark 训练和预测
|
|--- sql_file
|       01_get_userid_appnm_matrix_v3.sql  # 整理数据为（udid, app）格式，且用唯一整数表示
|       03_output_all_user_all_app_score_v3.sql  # 将结果的整数重新还原为字符串
```

#### 参考：

[详解矩阵分解算法在推荐系统中的应用(1) | 鹅厂实战](https://zhuanlan.zhihu.com/p/69662980)

