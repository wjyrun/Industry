# encoding: utf-8
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf()

warehouse_location = '/user/hive/warehouse/'

spark = SparkSession \
    .builder \
    .appName("panshi") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

##中介机构集中区域
#省份城市映射表
spark.read.csv("/user/wangjinyang/data/panshi/company_county.csv",sep=',',header=True)\
    .createOrReplaceTempView("county")
#中介机构
tagscount = spark.sql("select distinct bbd_qyxx_id,company_name from dw.qyxx_tags where dt='20170523' and tag='地产中介' limit 100")\
    .count()
	
if tagscount == 0:
    os._exit(0)

spark.sql("select distinct bbd_qyxx_id,company_name from dw.qyxx_tags where dt='20170523' and tag='地产中介' limit 10").show()

spark.stop()