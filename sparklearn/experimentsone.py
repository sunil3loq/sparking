__author__ = 'sunil'

from pyspark import SparkContext
from pyspark import sql
from pyspark import SQLContext

sc=SparkContext('local','jaffa')

sqc=SQLContext(sc)

#idea is to read the csv directly in to the dataframe of spark

#defining the schema
#msisdn,SongUniqueCode,Duration,Circle,DATE,DNIS,MODE,businesscategory
#9037991838,Hun-14-63767,202,Kolkata,10/1/2014,59090,,HindiTop20

mySchema=sql.types.StructType([
                        sql.types.StructField("msisdn",sql.types.StringType(),False),
                        sql.types.StructField("songid",sql.types.StringType(),False),
                        sql.types.StructField("duration",sql.types.IntegerType(),True),
                        sql.types.StructField("Circle",sql.types.StringType(),True),
                        sql.types.StructField("date",sql.types.StringType(),True),
                        sql.types.StructField("mode",sql.types.StringType(),True),
                        sql.types.StructField("businesscategory",sql.types.StringType(),True)
                        ])

transdf=sqc.load(source="com.databricks.spark.csv",path ="file:///home/loq/sunil/spark/content_data.csv",schema=mySchema)

transdf.take(2)

#reading the testfile way
'''
transrdd=sc.textFile("file:///home/loq/sunil/spark/content_data.csv").\
            map(lambda x: x.split(',')).\
            map(lambda y: sql.Row(msisdn=y[0],songid=y[1],duration=y[2],circle=y[3],businesscategory=y[7]))

print transrdd.take(2)
'''