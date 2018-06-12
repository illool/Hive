--@Name:TDM_ML_SF_GDSEARCH_D
--@Description:商品浏览信息汇总表
--@Type:日汇总,GDS_ID,CITY_CD
--@Target:SOPDM.TDM_ML_SF_GDSEARCH_D
--@Source:SCPDM.GYLTS_ML_gds_SELECTED
--@Source:SOPDM.TDM_ML_BR_TRMNL_BASE_VISIT_D
--@Source:SOPDM.TDM_ML_BR_BASE_VISIT_D
--@Author:17093220
--@CreateDate:2018-5-24
--@ModifyBy:
--@ModifyDate:
--@ModifyDesc:
--@Copyright Suning

-- 设置作业名
SET mapred.job.name = TDM_ML_SF_GDSEARCH_D(${hivevar:statis_date});
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

--@2.创建目标表TDM_ML_SF_GDSEARCH_D
DROP TABLE IF EXISTS TDM_ML_SF_GDSEARCH_D${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_GDSEARCH_D(
GDS_ID STRING, --商品id
CITY_CD STRING, --所属城市
TOT_QTY INT, --搜索销售数量
SEARCH_VISIT_PV INT, --搜索pv数量
SEARCH_VISIT_UV INT, --搜索pv数量
ETL_TIME STRING --处理时间
) PARTITIONED BY(STATIS_DATE STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE TDM_ML_SF_GDSEARCH_D PARTITION(STATIS_DATE='${hivevar:statis_date}')
SELECT temp1.GDS_ID AS GDS_ID,
       temp1.CITY_CD AS CITY_CD,
       SUM(temp1.TOT_QTY) AS TOT_QTY,
       SUM(temp1.search_visit_pv) AS search_visit_pv,
       SUM(temp1.search_visit_uv) AS search_visit_uv,
       FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS ETL_TIME
  FROM (SELECT t1.GDS_ID,
               t1.CITY_CD,
               t1.VISIT_ID,
               t1.TOT_QTY,
               t2.search_visit_pv,
               t2.search_visit_uv
          FROM (SELECT GDS_ID,
                       CITY_CD,
                       VISIT_ID,
                       TOT_QTY
                  FROM SOPDM.TDM_ML_OR_ORDER_D AS o 
                  LEFT SEMI JOIN
                       (SELECT GDS_CD
                          FROM SCPDM.GYLTS_ML_gds_SELECTED
                       ) d 
                    ON o.GDS_ID=d.gds_cd
                 WHERE STATIS_DATE='${hivevar:statis_date}'
                   AND GDS_ID!='-'
                   AND VISIT_ID!='-'
                   AND BUSI_TP_ID IN ('Z','-')
               ) AS t1
         INNER JOIN --基础流量表中关键词不为空的信息
               (SELECT GDS_ID,
                       VISIT_ID,
                       CITY_CD,
                       SUM(search_visit_pv) AS search_visit_pv,
                       SUM(search_visit_uv) AS search_visit_uv
                FROM(

                    SELECT GDS_ID,
                         VISIT_ID,
                         CITY_CD,
                         COUNT(PV_ID) AS search_visit_pv,
                         COUNT(DISTINCT VISITOR_ID) AS search_visit_uv
                    FROM SOPDM.TDM_ML_BR_TRMNL_BASE_VISIT_D tbv 
                    LEFT semi JOIN
                         (SELECT GDS_CD
                            FROM SCPDM.GYLTS_ML_gds_SELECTED
                         ) d 
                      ON tbv.GDS_ID=d.gds_cd
                    WHERE STATIS_DATE='${hivevar:statis_date}'
                     AND FROM_KEY_WORD != '-'
                     AND GDS_ID!=''
                     AND GDS_ID!='-'
                     AND VISIT_ID!='-'
                     AND VISIT_ID!=''
                     AND CITY_CD!='-'
                     AND CITY_CD!=''
                     AND BUSI_TP_ID IN ('Z-SHIP','-')
                     GROUP BY GDS_ID,
                              CITY_CD,
                              VISIT_ID

                 UNION ALL --移动流量表中关键词不为空的信息

                    SELECT GDS_ID,
                         VISIT_ID,
                         CITY_CD,
                         COUNT(PV_ID) AS search_visit_pv,
                         COUNT(DISTINCT VISITOR_ID) AS search_visit_uv
                    FROM SOPDM.TDM_ML_BR_BASE_VISIT_D bv 
                    LEFT semi JOIN
                         (SELECT GDS_CD
                            FROM SCPDM.GYLTS_ML_gds_SELECTED
                         ) d 
                      ON bv.GDS_ID=d.gds_cd
                    WHERE STATIS_DATE='${hivevar:statis_date}'
                     AND FROM_KEY_WORD != '-'
                     AND GDS_ID!=''
                     AND GDS_ID!='-'
                     AND VISIT_ID!='-'
                     AND VISIT_ID!=''
                     AND CITY_CD!='-'
                     AND CITY_CD!=''
                     AND BUSI_TP_ID IN ('Z-SHIP','-')
                     GROUP BY GDS_ID,
                             CITY_CD,
                             VISIT_ID

                    ) t
                GROUP BY GDS_ID,
                         CITY_CD,
                         VISIT_ID
               ) AS t2 
            ON t1.GDS_ID = t2.GDS_ID
           AND t1.CITY_CD = t2.CITY_CD
           AND t1.VISIT_ID = t2.VISIT_ID
       ) AS temp1
   GROUP BY temp1.GDS_ID,
            temp1.CITY_CD;
