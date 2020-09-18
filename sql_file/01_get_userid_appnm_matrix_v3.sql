-- 创建 udid appname 表
drop table if exists temp.evany_udid_applist;
create table temp.evany_udid_applist stored as parquet as

select a.udid
  ,upper(regexp_replace(b.app_name,'[^A-Z^a-z^0-9^\\u4e00-\\u9fa5]','')) as app_name

from(
    select udid 
    from udid_table
    where datediff(to_date(current_date()), ymd)<=30
    and default.is_notempty(udid)
    group by udid
) a

join(
    select udid,app_name
    from udid_app_table
    where default.is_notempty(udid)
    and default.is_notempty(app_name)
    group by udid,app_name
) b

on a.udid=b.udid;


-- 创建 udid uniqeid 字典对应表
drop table if exists temp.evany_udid_uniqid_dict;
create table temp.evany_udid_uniqid_dict stored as parquet as
select cast((row_number() over()) as bigint) as uniqid
  ,a.udid
from (
  select udid 
  from temp.evany_udid_applist
  where default.is_notempty(udid)
  group by udid
) a;


-- 创建 appname uniqeid 字典对应表
drop table if exists temp.evany_applist_uniqid_dict;
create table temp.evany_applist_uniqid_dict stored as parquet as
select cast((row_number() over()) as bigint) as uniqid
  ,a.app_name
from (
  select app_name
  from temp.evany_udid_applist
  where default.is_notempty(app_name)
  group by app_name
) a;


-- 将 udid appname 都转化为唯一的 int 类型 id
drop table if exists temp.evany_udid_applist_intid;
create table temp.evany_udid_applist_intid stored as parquet as
select sub.udid_uniqid
  ,d2.uniqid as app_uniqid

from (
  select * 
  from temp.evany_applist_uniqid_dict
) d2

join (
  select d1.uniqid as udid_uniqid
    ,l.app_name

  from (
    select * 
    from temp.evany_udid_uniqid_dict
  ) d1

  join (
    select * 
    from temp.evany_udid_applist
  ) l

  on d1.udid=l.udid
) sub

on d2.app_name=sub.app_name;


-- 创建 训练后的 设备 app 得分
-- drop table if exists temp.evany_udid_appid_score;
-- create table temp.evany_udid_appid_score (
--     udid_unique    String
--     ,appid_unique  String
--     ,score         String
-- ) 
-- stored as parquet;


-- 创建 训练后的 udid身份编码
drop table if exists temp.evany_udid_embedding;
create table temp.evany_udid_embedding (
    udid_unique    String
    ,embed_vector  String
) 
stored as parquet;


-- 创建 训练后的 app编码
drop table if exists temp.evany_app_embedding;
create table temp.evany_app_embedding (
    appid_unique  String
    ,embed_vector String
) 
stored as parquet;


-- 创建 PCA后的 身份编码表
-- drop table if exists temp.evany_userid_embedding_PCA_vector;
-- create table temp.evany_userid_embedding_PCA_vector (
--     client_uid String
--     ,pcafeaturesx String
--     ,pcafeaturesy String
-- ) 
-- stored as parquet;
