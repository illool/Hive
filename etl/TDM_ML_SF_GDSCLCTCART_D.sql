--@Name:TDM_ML_SF_GDSCLCTCART_D
--@Description:商品浏览信息汇总表
--@Type:日汇总,GDS_ID,CITY_CD
--@Target:SOPDM.TDM_ML_SF_GDSCLCTCART_D
--@Source:SCPDM.GYLTS_ML_gds_SELECTED
--@Source:SOPDM.TDM_ML_BR_BASE_VISIT_D
--@Source:BI_SOR.TSOR_BR_BASE_CLICK_D_IST
--@Source:BI_SOR.TSOR_BR_TRMNL_CLICK_D_IST
--@Author:17093220
--@CreateDate:2018-5-25
--@ModifyBy:
--@ModifyDate:
--@ModifyDesc:
--@Copyright Suning
-- 设置作业名
SET mapred.job.name = TDM_ML_SF_GDSCLCTCART_D(${hivevar:statis_date});
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
DROP TABLE IF EXISTS TDM_ML_SF_GDSCLCTCART_D${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_GDSCLCTCART_D(
GDS_ID STRING, --商品id
CITY_CD STRING, --所属城市
P_CLCT_UV_NUM INT, --收藏数量
CART_FLAG_UV_NUM INT, --加购物车数量
ETL_TIME STRING --处理时间
) PARTITIONED BY(STATIS_DATE STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE TDM_ML_SF_GDSCLCTCART_D PARTITION(STATIS_DATE='${hivevar:statis_date}')
SELECT T.GDS_ID,
       T.CITY_CD,
       COUNT(DISTINCT IF(T.P_CLCT_FLAG=1,VISITOR_ID,NULL)) AS P_CLCT_UV,
       COUNT(DISTINCT IF(T.CART_FLAG=1,VISITOR_ID,NULL)) AS CART_FLAG_UV,
       FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS ETL_TIME
FROM
  (SELECT T1.PV_ID,
          T1.VISITOR_ID,
          T1.CITY_CD,
          T1.GDS_ID,
          T2.P_CLCT_FLAG,
          T2.CART_FLAG
   FROM
      (SELECT  PV_ID,
              VISITOR_ID,
              CITY_CD,
              GDS_ID
      FROM SOPDM.TDM_ML_BR_BASE_VISIT_D AS a
      LEFT semi JOIN (
        SELECT gds_cd
        FROM SCPDM.GYLTS_ML_gds_SELECTED
      ) b ON a.GDS_ID=b.gds_cd
      WHERE STATIS_DATE = '${hivevar:statis_date}' AND BUSI_TP_ID IN ('Z-SHIP','-') 
      ) T1
   INNER JOIN
     (SELECT
            PV_ID,
            MAX(IF(PRMTR_ID_1 IN ('shoucang01','basic') AND PRMTR_ID_2 IN ('gmq','favorite'),1,0)) AS P_CLCT_FLAG,
            MAX(IF(PRMTR_ID_1='shoucang02' AND PRMTR_ID_2='shop',1,0)) AS S_CLCT_FLAG,
            MAX(IF(PRMTR_ID_1 IN ('buy01','buy03') AND PRMTR_ID_2 IN ('gmq','tab'),1,0)) AS CART_FLAG
      FROM BI_SOR.TSOR_BR_BASE_CLICK_D_IST
      WHERE STATIS_DATE='${hivevar:statis_date}'
      GROUP BY PV_ID
      ) T2 
   ON T1.PV_ID=T2.PV_ID

   UNION ALL 

   SELECT T1.PV_ID,
          T1.VISITOR_ID,
          T1.CITY_CD,
          T1.GDS_ID,
          T2.P_CLCT_FLAG,
          T2.CART_FLAG
   FROM
     (SELECT PV_ID,
             VISITOR_ID,
             CITY_CD,
             GDS_ID
      FROM SOPDM.TDM_ML_BR_TRMNL_BASE_VISIT_D AS a
      LEFT semi JOIN (
        SELECT gds_cd
        FROM SCPDM.GYLTS_ML_gds_SELECTED
      ) b ON a.GDS_ID=b.gds_cd
      WHERE STATIS_DATE = '${hivevar:statis_date}') T1
   INNER JOIN
     (SELECT TRMNL_TP_ID,
             CLNT_ID,
             VISIT_ID,
             VISIT_ORDER,
             MAX(IF(CLICK_NUM IN ('121316','780801','1010106','1010202','1010301','1200220','1210135'),1,0)) AS P_CLCT_FLAG,
             0 AS S_CLCT_FLAG,
             MAX(IF(CLICK_NUM IN ('121312','1210138','1210709'),1,0)) AS CART_FLAG
      FROM BI_SOR.TSOR_BR_TRMNL_CLICK_D_IST
      WHERE STATIS_DATE='${hivevar:statis_date}'
      GROUP BY TRMNL_TP_ID,
               CLNT_ID,
               VISIT_ID,
               VISIT_ORDER
     ) T2 
     ON T1.PV_ID=CONCAT(T2.TRMNL_TP_ID,T2.CLNT_ID,T2.VISIT_ID,T2.VISIT_ORDER)
     ) T
GROUP BY T.GDS_ID,T.CITY_CD;
