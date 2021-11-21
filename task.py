"""import findspark
findspark.init()
import time
from pyspark import SparkContext,SparkConf
from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count
import sys
import requests

#conf=SparkConf()
#conf.setAppName("ML_with_Spark")
sc=SparkContext(appName="test")
spark=SparkSession(sc)
ssc=StreamingContext(sc,2)

ssc.checkpoint("checkPoint")

datastream=ssc.socketTextStream("localhost",6100)
datastream.pprint()
df=spark.read.json(datastream)
df.show()
ssc.start()
ssc.awaitTermination(2)
ssc.stop()

"""

import sys
import re

from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import operator



#sqlContext = SQLContext(sc)

#model_inputs = sys.argv[1]


def main():
	
	sc = SparkContext(appName="test")
	ssc = StreamingContext(sc, 2)
	record=ssc.socketTextStream("localhost",6100)
	record.flatMap(lambda line:line.split(" ")).pprint()
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
