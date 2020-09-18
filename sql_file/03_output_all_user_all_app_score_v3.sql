-- 全体用户已安装app预测分数  暂时停掉
-- create table if not exists evany_userid_appname_score_base_applist_enhance
--     (
--     udid                            String            comment    '设备id'
--     ,app_name                       String            comment    'app名称'
--     ,udid_install_app_score         decimal(31,30)    comment    '该id的该app对应的分数'
--     ) partitioned by(update_time string)
-- row format delimited fields terminated by '\t';

-- insert overwrite table evany_userid_appname_score_base_applist_enhance partition(update_time = '${hivevar:UPDATE_TIME}')

-- select d.udid
--   ,c.app_name
--   ,cast(d.score as decimal(31,30)) as udid_install_app_score

-- from (
--   select *
--   from temp.evany_applist_uniqid_dict
-- ) c

-- join (
--   select a.udid
--     ,b.appid_unique
--     ,b.score

--   from (
--     select *
--     from temp.evany_udid_uniqid_dict
--   ) a

--   join (
--     select * 
--     from temp.evany_udid_appid_score
--   ) b

--   on a.uniqid=b.udid_unique
-- ) d

-- on c.uniqid=d.appid_unique;


-- 用户身份编码 200维
create table if not exists evany_udid_embedding_base_applist_enhance
    (
    udid                            String            comment    '设备id'
    ,embedding_vector               String            comment    '身份编码200维'
    ) partitioned by(update_time string)
row format delimited fields terminated by '\t';

insert overwrite table evany_udid_embedding_base_applist_enhance partition(update_time = '${hivevar:UPDATE_TIME}')

select b.udid
  ,a.embed_vector as embedding_vector

from (
  select udid_unique,embed_vector
  from temp.evany_udid_embedding
) a

join (
  select uniqid,udid
  from temp.evany_udid_uniqid_dict
) b

on a.udid_unique=b.uniqid;


-- app编码 200维
create table if not exists evany_app_embedding_base_applist_enhance
    (
    app_name                        String            comment    'app名字'
    ,embedding_vector               String            comment    'app编码200维'
    ) partitioned by(update_time string)
row format delimited fields terminated by '\t';

insert overwrite table evany_app_embedding_base_applist_enhance partition(update_time = '${hivevar:UPDATE_TIME}')

select b.app_name
  ,a.embed_vector as embedding_vector

from (
  select appid_unique,embed_vector
  from temp.evany_app_embedding
) a

join (
  select uniqid,app_name
  from temp.evany_applist_uniqid_dict
) b

on a.appid_unique=b.uniqid;
