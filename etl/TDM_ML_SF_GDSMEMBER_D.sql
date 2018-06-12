
--@Name:TDM_ML_SF_GDSMEMBER_D
--@Description:商品会员信息汇总表
--@Type:日汇总,GDS_ID,CITY_CD
--@Target:SOPDM.TDM_ML_SF_GDSMEMBER_D
--@Source:SCPDM.GYLTS_ML_gds_SELECTED
--@Source:SOPDM.TDM_ML_OR_ORDER_D
--@Source:SOPDM.TDM_ML_MEMBER_INFO 
--@Author:17093220
--@CreateDate:2018-5-29
--@ModifyBy:
--@ModifyDate:
--@ModifyDesc:
--@Copyright Suning
-- 设置作业名
SET mapred.job.name = TDM_ML_SF_GDSMEMBER_D(${hivevar:statis_date});
-- 每个Map最大输入大小
set mapred.max.split.size = 16000000;
-- 每个Map最小输入大小
set mapred.min.split.size = 8000000;
-- 执行Map前进行小文件合并
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- 在Map-only的任务结束时合并小文件
set hive.merge.mapfiles = true;
-- 在Map-Reduce的任务结束时合并小文件
set hive.merge.mapredfiles = true;
-- 合并文件的大小
set hive.merge.size.per.task = 16000000;
set hive.merge.smallfiles.avgsize=16000000;
-- 设置reducce数量
set hive.exec.reducers.bytes.per.reducer=16000000;
-- hive自动根据sql，选择使用common join或者map join
set hive.auto.convert.join = true;
-- 允许并行执行
set hive.exec.parallel=true;

--@1.切换schema
USE SOPDM;

--@2.创建目标表TDM_ML_SF_GDSMEMBER_D
DROP TABLE IF EXISTS TDM_ML_SF_GDSMEMBER_D${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_GDSMEMBER_D(GDS_ID STRING, --商品id
CITY_CD STRING, --所属城市
MEMBERS INT, --会员数量
NEW INT, --新会员
V3 INT, --v3会员
V2 INT, --v2会员
V1 INT, --v1会员
ORTHER INT, --其他等级会员
VN INT, --无法显示会员等级会员
AR1 INT, --40岁-49岁
AR2 INT, --19岁-24岁
AR3 INT, --18岁以下
AR4 INT, --'-'
AR5 INT, --35岁-39岁
AR6 INT, --50岁-59岁
AR7 INT, --34岁-39岁
AR8 INT, --25岁-29岁
AR9 INT, --60岁以上
AR10 INT, --无年龄段信息
F INT, --女性会员
M INT, --男性会员
N INT, --无性别会员
ETL_TIME STRING --处理时间
) PARTITIONED BY(STATIS_DATE STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE TDM_ML_SF_GDSMEMBER_D PARTITION(STATIS_DATE='${hivevar:statis_date}')
select t1.GDS_ID,t1.CITY_CD,
count(t2.MEMBER_ID) AS members,
SUM(CASE WHEN t2.MEM_LVL = '161000000100' THEN 1 ELSE 0 END) as new,
SUM(CASE WHEN t2.MEM_LVL = '161000000130' THEN 1 ELSE 0 END) as v3,
SUM(CASE WHEN t2.MEM_LVL = '161000000120' THEN 1 ELSE 0 END) as v2,
SUM(CASE WHEN t2.MEM_LVL = '161000000110' THEN 1 ELSE 0 END) as v1,
SUM(CASE WHEN t2.MEM_LVL = '-' THEN 1 ELSE 0 END) as orther,
SUM(CASE WHEN t2.MEM_LVL != '-'
    AND t2.MEM_LVL != '161000000100'
    AND t2.MEM_LVL != '161000000130'
    AND t2.MEM_LVL != '161000000120'
    AND t2.MEM_LVL != '161000000110'
    THEN 1 ELSE 0 END) as vn,
SUM(CASE WHEN t2.AGE_RANGE = '40岁-49岁' THEN 1 ELSE 0 END) as ar1,
SUM(CASE WHEN t2.AGE_RANGE = '19岁-24岁' THEN 1 ELSE 0 END) as ar2,
SUM(CASE WHEN t2.AGE_RANGE = '18岁以下' THEN 1 ELSE 0 END) as ar3,
SUM(CASE WHEN t2.AGE_RANGE = '-' THEN 1 ELSE 0 END) as ar4,
SUM(CASE WHEN t2.AGE_RANGE = '35岁-39岁' THEN 1 ELSE 0 END) as ar5,
SUM(CASE WHEN t2.AGE_RANGE = '50岁-59岁' THEN 1 ELSE 0 END) as ar6,
SUM(CASE WHEN t2.AGE_RANGE = '34岁-39岁' THEN 1 ELSE 0 END) as ar7,
SUM(CASE WHEN t2.AGE_RANGE = '25岁-29岁' THEN 1 ELSE 0 END) as ar8,
SUM(CASE WHEN t2.AGE_RANGE = '60岁以上' THEN 1 ELSE 0 END) as ar9,
SUM(CASE WHEN t2.AGE_RANGE != '60岁以上' 
    AND t2.AGE_RANGE != '25岁-29岁'
    AND t2.AGE_RANGE != '34岁-39岁'
    AND t2.AGE_RANGE != '50岁-59岁'
    AND t2.AGE_RANGE != '35岁-39岁'
    AND t2.AGE_RANGE != '-'
    AND t2.AGE_RANGE != '18岁以下'
    AND t2.AGE_RANGE != '19岁-24岁'
    AND t2.AGE_RANGE != '40岁-49岁'
    THEN 1 ELSE 0 END) as ar10,
SUM(CASE WHEN t2.GENDER = 'F' THEN 1 ELSE 0 END) as f,
SUM(CASE WHEN t2.GENDER = 'M' THEN 1 ELSE 0 END) as m,
SUM(CASE WHEN t2.GENDER = 'N' THEN 1 ELSE 0 END) as n,
FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS ETL_TIME
from 
(
  SELECT a.GDS_ID,a.CITY_CD,a.MEMBER_ID FROM SOPDM.TDM_ML_OR_ORDER_D a
  LEFT semi JOIN (
    SELECT gds_cd
    FROM SCPDM.GYLTS_ML_gds_SELECTED
  ) b ON a.GDS_ID=b.gds_cd
  WHERE STATIS_DATE='${hivevar:statis_date}' AND BUSI_TP_ID IN ('Z','-')
  group by a.GDS_ID,a.CITY_CD,a.MEMBER_ID
) t1
  INNER JOIN 
  (
  SELECT MEMBER_ID,MEM_LVL,AGE_RANGE,GENDER FROM SOPDM.TDM_ML_MEMBER_INFO 
  )t2
ON t1.MEMBER_ID = t2.MEMBER_ID
group by t1.GDS_ID,t1.CITY_CD;
