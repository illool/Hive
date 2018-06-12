--@Name:TDM_ML_SF_SAL_FS
--@Description:商品销售信息汇总表
--@Type:日汇总,GDS_ID,CITY_CD,CHNL_CD
--@Target:SOPDM.TDM_ML_SF_SAL_FS
--@Source:SCPDM.TDM_ML_OR_ORDER_D
--@Source:BI_SOR.TSOR_OR_OMS_DEAL_TYPE_INFO_D
--@Author:17093220
--@CreateDate:2018-5-29
--@ModifyBy:
--@ModifyDate:
--@ModifyDesc:
--@Copyright Suning
-- 设置作业名
SET mapred.job.name = TDM_ML_SF_SAL_FS(${hivevar:statis_date});
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
--@2.创建目标表 TDM_ML_SF_SAL_FS
DROP TABLE IF EXISTS TDM_ML_SF_SAL_FS${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_SAL_FS (

GDS_CD          STRING  ,-- 店铺编码
CITY_CD         STRING  ,-- 城市编码
CHNL_CD           STRING  ,-- 渠道编码
sale_cnt        INT     ,-- 下单件数
sale_promotion_cnt    INT     ,-- 促销下单
sale_promotion_num    INT     ,-- 促销成交
goods_rejected_num      DOUBLE  ,-- 退货件数（率）
sale_member_num     DOUBLE  ,-- 成交会员数
gds_price           DOUBLE  ,-- 销售单价
sale_price        DOUBLE  --  成交单价

) PARTITIONED BY (STATIS_DATE STRING)  STORED AS RCFILE;

DROP TABLE IF EXISTS TDM_ML_SF_SAL_FS00_${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_SAL_FS00_${hivevar:statis_date} STORED AS RCFILE AS
SELECT
  A0.statis_date,
  A0.GDS_ID,
  A0.CITY_CD,
  A0.CHNL_CD,
  A0.TOT_QTY,
  A0.OMS_ORDER_ITEM_ID,
  A0.BUSI_TP_ID,
  A0.RFD_TIME,
  A0.member_id,
  A0.TOT_AMNT,
  A0.SHLD_AMNT,
  A0.order_time,
  A0.pay_time
FROM sopdm.TDM_ML_OR_ORDER_D A0
INNER JOIN  scpdm.gylts_ml_gds_selected B0
ON A0.GDS_ID = B0.GDS_CD
where A0.statis_date=${hivevar:statis_date} and (A0.BUSI_TP_ID='Z' or A0.BUSI_TP_ID='-');

DROP TABLE IF EXISTS TDM_ML_SF_SAL_FS01_${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_SAL_FS01_${hivevar:statis_date} STORED AS RCFILE AS
SELECT
  GDS_ID,
  CITY_CD,
  CHNL_CD,
  SUM(TOT_AMNT)/SUM(TOT_QTY) as gds_price,
  SUM(SHLD_AMNT)/SUM(TOT_QTY) as sale_price,
  sum(TOT_QTY) as sale_cnt
FROM sopdm.TDM_ML_SF_SAL_FS00_${hivevar:statis_date}
where statis_date=${hivevar:statis_date} group by GDS_ID,CITY_CD,CHNL_CD;

DROP TABLE IF EXISTS TDM_ML_SF_SAL_FS02_${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_SAL_FS02_${hivevar:statis_date} STORED AS RCFILE AS
SELECT
  p1.GDS_ID,
  p1.CITY_CD,
  p1.CHNL_CD,
  count(p1.OMS_ORDER_ITEM_ID) as sale_promotion_cnt,
  SUM(p1.TOT_QTY) as sale_promotion_num
FROM (select A.GDS_ID,A.OMS_ORDER_ITEM_ID,A.TOT_QTY,A.CITY_CD,A.CHNL_CD,A.BUSI_TP_ID from TDM_ML_SF_SAL_FS00_${hivevar:statis_date} A
inner join (select ORDER_ITEM_ID from BI_SOR.TSOR_OR_OMS_DEAL_TYPE_INFO_D where statis_date=${hivevar:statis_date} and PROMOTION_NUM IS NOT NULL and DEAL_TYPE in('02','03') group by ORDER_ITEM_ID) B
ON A.OMS_ORDER_ITEM_ID=B.ORDER_ITEM_ID
where A.statis_date=${hivevar:statis_date} and regexp_replace(substring(A.order_time,1,10),'-','')=A.statis_date) p1
group by p1.GDS_ID,p1.CITY_CD,p1.CHNL_CD;

DROP TABLE IF EXISTS TDM_ML_SF_SAL_FS03_${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_SAL_FS03_${hivevar:statis_date} STORED AS RCFILE AS
SELECT
  GDS_ID,
  CITY_CD,
  CHNL_CD,
  SUM(TOT_QTY) as goods_rejected_num
FROM SOPDM.TDM_ML_SF_SAL_FS00_${hivevar:statis_date}
Where statis_date = from_unixtime(to_unix_timestamp(to_date(RFD_TIME),'yyyy-MM-dd'),'yyyyMMdd') 
and statis_date=${hivevar:statis_date}
group by GDS_ID,CITY_CD,CHNL_CD;

DROP TABLE IF EXISTS TDM_ML_SF_SAL_FS04_${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_SAL_FS04_${hivevar:statis_date} STORED AS RCFILE AS
Select 
  A.GDS_ID, 
  A.CITY_CD,  
  A.CHNL_CD,
  count(distinct A.member_id) as sale_member_num  
FROM TDM_ML_SF_SAL_FS00_${hivevar:statis_date} A
where A.statis_date=${hivevar:statis_date} and regexp_replace(substring(A.pay_time,1,10),'-','')=A.statis_date
Group by A.GDS_ID,A.CITY_CD,A.CHNL_CD;

INSERT OVERWRITE TABLE TDM_ML_SF_SAL_FS PARTITION (STATIS_DATE='${hivevar:statis_date}')
SELECT
  GDS_ID,
  CITY_CD,
  CHNL_CD,
  SUM(sale_cnt) sale_cnt,
  SUM(sale_promotion_cnt) sale_promotion_cnt,
  SUM(sale_promotion_num) sale_promotion_num,
  SUM(goods_rejected_num) goods_rejected_num,
  SUM(sale_member_num) sale_member_num,
  SUM(gds_price) gds_price,
  SUM(sale_price) sale_price
FROM(
    SELECT
    GDS_ID,
    CITY_CD,
    CHNL_CD,
    sale_cnt,   
    0 as sale_promotion_cnt,
    0 as sale_promotion_num,
    0 as goods_rejected_num,
    0 as sale_member_num,
    gds_price,
      sale_price
  FROM TDM_ML_SF_SAL_FS01_${hivevar:statis_date} t1
  union all
    SELECT
    GDS_ID,
    CITY_CD,
    CHNL_CD,
    0 as sale_cnt,  
    sale_promotion_cnt,
    sale_promotion_num,
    0 as goods_rejected_num,
    0 as sale_member_num,
    0 as gds_price,
      0 as sale_price
  FROM TDM_ML_SF_SAL_FS02_${hivevar:statis_date}  t2
  union all
  SELECT
    GDS_ID,
    CITY_CD,
    CHNL_CD,
    0 as sale_cnt,  
    0 as sale_promotion_cnt,
    0 as sale_promotion_num,
    goods_rejected_num,
    0 as sale_member_num,
    0 as gds_price,
      0 as sale_price
  FROM TDM_ML_SF_SAL_FS03_${hivevar:statis_date} t3
  union all
  SELECT
    GDS_ID,
    CITY_CD,
    CHNL_CD,
    0 as sale_cnt,  
    0 as sale_promotion_cnt,
    0 as sale_promotion_num,
    0 as goods_rejected_num,
    sale_member_num,
    0 as gds_price,
      0 as sale_price
  FROM TDM_ML_SF_SAL_FS04_${hivevar:statis_date} t4
  ) TT
  group by GDS_ID,CITY_CD,CHNL_CD;
