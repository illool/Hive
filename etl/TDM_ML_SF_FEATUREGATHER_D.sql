--@Name:TDM_ML_SF_FEATUREGATHER_D
--@Description:商品会员信息汇总表
--@Type:日汇总,GDS_ID,CITY_CD,CHNL_CD
--@Target:SOPDM.TDM_ML_SF_FEATURE_D
--@Source:SCPDM.TDM_ML_SF_GDSVISIT_D
--@Source:SCPDM.TDM_ML_SF_GDSCLCTCART_D
--@Source:SCPDM.TDM_ML_SF_GDSEARCH_D
--@Source:SCPDM.TDM_ML_SF_GDSMEMBER_D
--@Source:SCPDM.TDM_ML_SF_SAL_FS
--@Author:17093220
--@CreateDate:2018-5-29
--@ModifyBy:
--@ModifyDate:
--@ModifyDesc:
--@Copyright Suning
-- 设置作业名
SET mapred.job.name = TDM_ML_SF_FEATUREGATHER_D(${hivevar:statis_date});
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

--@2.创建目标表TDM_ML_SF_FEATUREGATHER_D
DROP TABLE IF EXISTS TDM_ML_SF_FEATUREGATHER_D${hivevar:statis_date};
CREATE TABLE IF NOT EXISTS TDM_ML_SF_FEATUREGATHER_D(GDS_ID STRING, --商品id
CITY_CD STRING, --所属城市
--FROM TDM_ML_SF_SAL_FS
CHNL_CD STRING ,-- 渠道编码
SALE_CNT INT,-- 下单件数
SALE_PROMOTION_CNT INT,-- 促销下单
SALE_PROMOTION_NUM INT,-- 促销成交
GOODS_REJECTED_NUM DOUBLE,-- 退货件数(率)
SALE_MEMBER_NUM DOUBLE,-- 成交会员数
GDS_PRICE DOUBLE,-- 销售单价
SALE_PRICE DOUBLE, -- 成交单价
--FROM TDM_ML_SF_GDSVISIT_D
PV_QTY INT, --PV数量 
VISITOR_NUM INT, --访客数
OOS_PV_QTY INT, --缺货pv数量
ONE_PV_PUV_NUM INT, --1PV访问次数
VISITORS INT, --访问次数
OLD_UV INT, --回访客数
LOSS_RATE FLOAT, --跳失率(1PV访问次数/访问次数)
--FROM TDM_ML_SF_GDSCLCTCART_D
P_CLCT_UV_NUM INT, --收藏数量
CART_FLAG_UV_NUM INT, --加购物车数量
--FROM TDM_ML_SF_GDSEARCH_D
TOT_QTY INT, --搜索销售数量
SEARCH_VISIT_PV INT, --搜索pv数量
SEARCH_VISIT_UV INT, --搜索pv数量
--FROM TDM_ML_SF_GDSMEMBER_D
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


INSERT OVERWRITE TABLE TDM_ML_SF_FEATUREGATHER_D PARTITION(STATIS_DATE='${hivevar:statis_date}')
SELECT 
a.GDS_CD, --商品id
a.CITY_CD, --所属城市
--FROM TDM_ML_SF_SAL_FS a
a.CHNL_CD ,-- 渠道编码
a.SALE_CNT,-- 下单件数
a.SALE_PROMOTION_CNT,-- 促销下单
a.SALE_PROMOTION_NUM,-- 促销成交
a.GOODS_REJECTED_NUM,-- 退货件数(率)
a.SALE_MEMBER_NUM,-- 成交会员数
a.GDS_PRICE,-- 销售单价
a.SALE_PRICE, -- 成交单价
--FROM TDM_ML_SF_GDSVISIT_D b
b.PV_QTY, --PV数量 
b.VISITOR_NUM, --访客数
b.OOS_PV_QTY, --缺货pv数量
b.ONE_PV_PUV_NUM, --1PV访问次数
b.VISITORS, --访问次数
b.OLD_UV, --回访客数
b.LOSS_RATE, --跳失率(1PV访问次数/访问次数)
--FROM TDM_ML_SF_GDSCLCTCART_D c
c.P_CLCT_UV_NUM, --收藏数量
c.CART_FLAG_UV_NUM, --加购物车数量
--FROM TDM_ML_SF_GDSEARCH_D d
d.TOT_QTY, --搜索销售数量
d.SEARCH_VISIT_PV, --搜索pv数量
d.SEARCH_VISIT_UV, --搜索pv数量
--FROM TDM_ML_SF_GDSMEMBER_D e
e.MEMBERS, --会员数量
e.NEW, --新会员
e.V3, --v3会员
e.V2, --v2会员
e.V1, --v1会员
e.ORTHER, --其他等级会员
e.VN, --无法显示会员等级会员
e.AR1, --40岁-49岁
e.AR2, --19岁-24岁
e.AR3, --18岁以下
e.AR4, --'-'
e.AR5, --35岁-39岁
e.AR6, --50岁-59岁
e.AR7, --34岁-39岁
e.AR8, --25岁-29岁
e.AR9, --60岁以上
e.AR10, --无年龄段信息
e.F, --女性会员
e.M, --男性会员
e.N,--无性别会员
FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS ETL_TIME

 FROM 

(
  SELECT 
    GDS_CD ,-- 商品编码
    CITY_CD ,--城市编码
    CHNL_CD ,-- 渠道编码
    SALE_CNT,-- 下单件数
    SALE_PROMOTION_CNT,-- 促销下单
    SALE_PROMOTION_NUM,-- 促销成交
    GOODS_REJECTED_NUM,-- 退货件数(率)
    SALE_MEMBER_NUM,-- 成交会员数
    GDS_PRICE,-- 销售单价
    SALE_PRICE -- 成交单价
  FROM TDM_ML_SF_SAL_FS WHERE STATIS_DATE='${hivevar:statis_date}' AND CHNL_CD='50'
 ) a

LEFT JOIN

(
  SELECT 
    GDS_ID ,-- 商品编码
    CITY_CD ,--城市编码
    PV_QTY, --PV数量 
    VISITOR_NUM, --访客数
    OOS_PV_QTY, --缺货pv数量
    ONE_PV_PUV_NUM, --1PV访问次数
    VISITORS, --访问次数
    OLD_UV, --回访客数
    LOSS_RATE --跳失率(1PV访问次数/访问次数)
  FROM TDM_ML_SF_GDSVISIT_D WHERE STATIS_DATE='${hivevar:statis_date}'
) b

ON a.GDS_CD = b.GDS_ID AND a.CITY_CD = b.CITY_CD

LEFT JOIN

(
  SELECT 
    GDS_ID ,-- 商品编码
    CITY_CD ,--城市编码
    P_CLCT_UV_NUM, --收藏数量
    CART_FLAG_UV_NUM --加购物车数量
  FROM TDM_ML_SF_GDSCLCTCART_D WHERE STATIS_DATE='${hivevar:statis_date}'
) c

ON a.GDS_CD = c.GDS_ID AND a.CITY_CD = c.CITY_CD

LEFT JOIN

(
  SELECT 
    GDS_ID ,-- 商品编码
    CITY_CD ,--城市编码
    TOT_QTY, --搜索销售数量
    SEARCH_VISIT_PV, --搜索pv数量
    SEARCH_VISIT_UV --搜索pv数量
  FROM TDM_ML_SF_GDSEARCH_D WHERE STATIS_DATE='${hivevar:statis_date}'
) d

ON a.GDS_CD = d.GDS_ID AND a.CITY_CD = d.CITY_CD

LEFT JOIN

(
  SELECT 
    GDS_ID ,-- 商品编码
    CITY_CD ,--城市编码
    MEMBERS, --会员数量
    NEW, --新会员
    V3, --v3会员
    V2, --v2会员
    V1, --v1会员
    ORTHER, --其他等级会员
    VN, --无法显示会员等级会员
    AR1, --40岁-49岁
    AR2, --19岁-24岁
    AR3, --18岁以下
    AR4, --'-'
    AR5, --35岁-39岁
    AR6, --50岁-59岁
    AR7, --34岁-39岁
    AR8, --25岁-29岁
    AR9, --60岁以上
    AR10, --无年龄段信息
    F, --女性会员
    M, --男性会员
    N --无性别会员
  FROM TDM_ML_SF_GDSMEMBER_D WHERE STATIS_DATE='${hivevar:statis_date}'
) e

ON a.GDS_CD = e.GDS_ID AND a.CITY_CD = e.CITY_CD;
