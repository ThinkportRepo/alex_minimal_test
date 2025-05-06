##### simple tests ##################################################
'''
* test create Spark Context and Spark Session
* test serialisation of data to rdd
* test serialisation of data to df
* test basic sparkSQL functions and aggregations
* test if data can be collected to master (show, count, collect)
* test if data can be read from s3 bucket
'''
####################################################################
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f


if __name__ == "__main__":
    """
        Usage: basic_test
    """
    spark = SparkSession\
        .builder\
        .appName("spark-simple-app")\
        .getOrCreate()

    sc=spark.sparkContext

    sc.setLogLevel('WARN')
    
    print("-----------------------------------------------------")
    print("Spark App starting ...")
    print("-----------------------------------------------------")



    url = "jdbc:postgresql://hive-postgresql.hive-dp.cluster.local:5432/hive"
    properties = {
        "user": "hive",
        "password": "hive",
        "driver": "org.postgresql.Driver"
    }
    table = "GLOBAL_PRIVS"

    df_pg = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .load()

    print("++ show db result")
    print("################################################")
    df_pg.show()
    
    print("-----------------------------------------------------")
    print("Spark App completed")
    print("-----------------------------------------------------")
    spark.stop()
