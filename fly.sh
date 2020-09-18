#!/bin/sh
source /etc/profile
export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"

work_folder=applist_enhance

update_time=`date "+%Y-%m-%d"`
yesterday_date=`date +%Y-%m-%d -d "-1 days"`

printf "=== %-15s  | %-25s ===\n" 'taskname' 'user id embedding base applist'
printf "=== %-15s  | %-25s ===\n" 'date' "`date +%Y-%m-%d`"
printf "=== %-15s  | %-25s ===\n" 'author' 'Evany'
printf "=== %-15s  | %-25s ===\n" 'introduction' 'T+1'

echo "========== Step 1. get_userid_appnm_matrix =========="
echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
s1_st=$(date "+%s")
beeline -u "xxx" --hivevar YESTERDAY_DATE="${yesterday_date}" -n xxx -p xxx -f ${work_folder}/sql_file/01_get_userid_appnm_matrix_v3.sql
s1_et=$(date "+%s")
s1_du=$((s1_et-s1_st))
echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
echo "========== Step 1. Finished with time ${s1_du} seconds =========="

echo -e "\n================================================================================\n"

echo "========== Step 2. train_ALS_MF_spark =========="
echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
s2_st=$(date "+%s")
#spark-submit \
#  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${work_folder}/python_file/log4j.properties" \
#  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${work_folder}/python_file/log4j.properties" \
#spark-submit \
#  --conf "spark.yarn.executor.memoryOverhead=4G" \
#  --conf "spark.default.parallelism=500" \
#  --conf "spark.sql.shuffle.partitions=400" \
#  --conf "spark.storage.blockManagerTimeoutIntervalMs=100000" \
#  --conf "spark.speculation=true" \
#  --conf "spark.driver.memory=15g" \
#  --conf "spark.storage.memoryFraction=0.8" \
#  --conf "spark.shuffle.memoryFraction=0.1" \
#  --master yarn \
#  --driver-memory 8g \
#  --executor-memory 5g \
#  --num-executors 50 \
#  --executor-cores 2 \
#  --queue root.intelli_algo.intelli_algo_rc \
#  ${work_folder}/python_file/02_train_ALS_MF_spark.py
/data/spark/spark-2.2.0/bin/spark-submit \
    --conf spark.yarn.queue=root.xxx\
    --conf spark.yarn.dist.files=file:/data/spark/spark-2.2.0/python/lib/pyspark.zip,file:/data/spark/spark-2.2.0/python/lib/py4j-0.10.4-src.zip\
    --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/local/anaconda3/envs/py2.7\
    --conf spark.executorEnv.PYTHONPATH=pyspark.zip:py4j-0.10.4-src.zip:/usr/bin/python\
    --conf spark.yarn.historyServer.address=http://xx.xx.xx.xx:xxx\
    --conf spark.eventLog.dir=hdfs://nameservice1/user/spark2.2.0/applicationHistory\
    --conf spark.driver.maxResultSize=1G\
    --num-executors 32\
    --executor-cores 1\
    --executor-memory 10G\
    --driver-memory 1G\
    --master=yarn\
    ${work_folder}/python_file/02_train_ALS_MF_spark.py
s2_et=$(date "+%s")
s2_du=$((s2_et-s2_st))
echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
echo "========== Step 2. Finished with time ${s2_du} seconds =========="

echo -e "\n================================================================================\n"

echo "========== Step 3. output_user_app_score =========="
echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
s3_st=$(date "+%s")
beeline -u "xxxx" --hivevar UPDATE_TIME="${update_time}" -n xxx -p xxx -f ${work_folder}/sql_file/03_output_all_user_all_app_score_v3.sql
s3_et=$(date "+%s")
s3_du=$((s3_et-s3_st))
echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
echo "========== Step 3. Finished with time ${s3_du} seconds =========="

#echo -e "\n================================================================================\n"
#
#echo "========== Step 4. k-means =========="
#echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#s4_st=$(date "+%s")
#s4_et=$(date "+%s")
#s4_du=$((s4_et-s4_st))
#echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#echo "========== Step 4. Finished with time ${s4_du} seconds =========="
#
#echo -e "\n================================================================================\n"

#echo "========== Step 5. PCA_2_2d_spark =========="
#echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#s5_st=$(date "+%s")
#spark-submit \
#  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${work_folder}/python_file/log4j.properties" \
#  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${work_folder}/python_file/log4j.properties" \
#  --conf "spark.yarn.executor.memoryOverhead=4G" \
#  --master yarn \
#  --deploy-mode client \
#  --driver-memory 4g \
#  --executor-memory 8g \
#  --num-executors 16 \
#  --executor-cores 4 \
#  --queue root.xxxx \
#  ${work_folder}/python_file/05_PCA_2d_vector_spark.py
#s5_et=$(date "+%s")
#s5_du=$((s5_et-s5_st))
#echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#echo "========== Step 5. Finished with time ${s5_du} seconds =========="
#
#echo -e "\n================================================================================\n"
#
#echo "========== Step 6. save_PCA_data_2_local =========="
#echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#s6_st=$(date "+%s")
#spark-submit \
#  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${work_folder}/python_file/log4j.properties" \
#  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${work_folder}/python_file/log4j.properties" \
#  --master local \
#  --queue root.xxxx \
#  ${work_folder}/python_file/06_save_PCA_data_local.py
#
#if [ -d "${work_folder}/cache_file/2d_data.csv" ];then
#    rm ${work_folder}/cache_file/2d_data.csv
#fi
#
#cat ${work_folder}/cache_file/pca_vec/*.csv > ${work_folder}/cache_file/2d_data.csv
#sed -i '1i\client_uid,pcafeaturesx,pcafeaturesy' ${work_folder}/cache_file/2d_data.csv
#s6_et=$(date "+%s")
#s6_du=$((s6_et-s6_st))
#echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#echo "========== Step 6. Finished with time ${s6_du} seconds =========="
#
#echo -e "\n================================================================================\n"
#
#echo "========== Step 7. plot PCA data =========="
#echo "========== Start Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#s7_st=$(date "+%s")
##source activate py3.6
#python ${work_folder}/python_file/07_plot_PCA_data.py ${work_folder}/cache_file/2d_data.csv ${work_folder}/cache_file/pca.png
##source deactivate py3.6
#s7_et=$(date "+%s")
#s7_du=$((s7_et-s7_st))
#echo "========== End Date `date '+%Y-%m-%d %H:%M:%S'` =========="
#echo "========== Step 7. Finished with time ${s7_du} seconds =========="
#
#echo -e "\n================================================================================\n"
