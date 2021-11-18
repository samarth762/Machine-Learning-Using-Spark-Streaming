import findspark
findspark.init()
import time
from pyspark import SparkContext,SparkConf
from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import count
import sys
import requests

#conf=SparkConf()
#conf.setAppName("ML_with_Spark")
sc=SparkContext(appName="test")

ssc=StreamingContext(sc,2)
ssc.checkpoint("checkPoint")

datastream=ssc.socketTextStream("localhost",6100)
datastream.pprint()
ssc.start()
ssc.awaitTermination(2)
ssc.stop()
