--@Name:TDM_ML_SF_GDSVISIT_D
--@Description:商品浏览信息汇总表
--@Type:日汇总,GDS_ID,CITY_CD
--@Target:SOPDM.TDM_SOP_SF_GDSVISIT_D
--@Source:SCPDM.GYLTS_ML_gds_SELECTED
--@Source:SOPDM.TDM_ML_BR_CUST_VISIT_D
--@Author:17093220
--@CreateDate:2018-5-24
--@ModifyBy:
--@ModifyDate:
--@ModifyDesc:
--@Copyright Suning

-- 设置作业名
SET mapred.job.name = TDM_ML_SF_GDSVISIT_D(${hivevar:statis_date});
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

--@2.创建目标表TDM_ML_SF_GDSVISIT_D
DROP TABLE IF EXISTS TDM_ML_SF_GDSVISIT_D${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_GDSVISIT_D(GDS_ID STRING, --商品id
CITY_CD STRING, --所属城市
PV_QTY INT, --PV数量 
VISITOR_NUM INT, --访客数
OOS_PV_QTY INT, --缺货pv数量
ONE_PV_PUV_NUM INT, --1PV访问次数
VISITORS INT, --访问次数
OLD_UV INT, --回访客数
LOSS_RATE FLOAT, --跳失率(1PV访问次数/访问次数)
ETL_TIME STRING --处理时间
) PARTITIONED BY(STATIS_DATE STRING) STORED AS RCFILE;


INSERT OVERWRITE TABLE TDM_ML_SF_GDSVISIT_D PARTITION(STATIS_DATE='${hivevar:statis_date}')
SELECT a.GDS_ID,
  a.CITY_CD,
  a.PV_QTY,
  a.VISITOR_NUM,
  a.OOS_PV_QTY,
  a.ONE_PV_PUV_FLAG,
  a.VISITORS,
  d.OLD_UV,
  cast(ONE_PV_PUV_FLAG as FLOAT)/cast(VISITORS as float) AS LOSS_RATE,
  FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS ETL_TIME
FROM (
  SELECT a.GDS_ID AS GDS_ID, a.CITY_CD AS CITY_CD, sum(a.PV_QTY) AS PV_QTY,--PV数量
  count(DISTINCT a.VISITOR_ID) AS VISITOR_NUM,--访客数
  sum(a.OOS_PV_QTY) AS OOS_PV_QTY,--缺货pv数量
  count(DISTINCT if(a.ONE_PV_PUV_FLAG=1,visit_id,NULL)) AS ONE_PV_PUV_FLAG,--1PV访问次数a
  count(DISTINCT a.VISIT_ID) AS VISITORS--访问次数
  FROM sopdm.TDM_ML_BR_CUST_VISIT_D AS a 
  LEFT semi JOIN (
    SELECT gds_cd
    FROM SCPDM.GYLTS_ML_gds_SELECTED
  ) b ON a.GDS_ID=b.gds_cd
  WHERE a.statis_date = ${hivevar:statis_date}
    AND a.BUSI_TP_ID IN ('Z-SHIP','-')
    AND a.GDS_ID != '-'
  GROUP BY a.GDS_ID,a.CITY_CD
) a
LEFT JOIN (
  SELECT gds_id,
    city_cd,
    sum(CASE WHEN old_visitor != 0 AND new_visitor != 0 THEN 1 ELSE 0 END) AS OLD_UV --回访客数
  FROM (
    SELECT gds_id,
      city_cd,
      visitor_id,
      max(CASE WHEN statis_date BETWEEN ${hivevar:statis_date_7d} AND ${hivevar:statis_date_1d} THEN 1 ELSE 0 END) old_visitor,
      max(CASE WHEN statis_date == ${hivevar:statis_date} THEN 1 ELSE 0 END) new_visitor
    FROM (
      SELECT gds_id,city_cd,visitor_id,statis_date
      FROM sopdm.TDM_ML_BR_CUST_VISIT_D
      WHERE statis_date BETWEEN ${hivevar:statis_date_7d} AND ${hivevar:statis_date} AND GDS_ID != '-' AND BUSI_TP_ID IN ('Z-SHIP','-')
    ) t1
    INNER JOIN ( 
      SELECT gds_cd FROM SCPDM.GYLTS_ML_gds_SELECTED 
    ) t2 ON t1.GDS_ID=t2.gds_cd
    GROUP BY gds_id,city_cd,visitor_id
  ) t
  GROUP BY gds_id,city_cd
) d ON a.gds_id = d.gds_id AND a.city_cd = d.city_cd;
