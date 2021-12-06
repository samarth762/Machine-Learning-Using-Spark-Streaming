import sys
import re
import json
from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import operator
import nltk
from nltk.corpus import stopwords




#sqlContext = SQLContext(sc)

#model_inputs = sys.argv[1]


def main():
	sc = SparkContext(appName="test")
	ssc = StreamingContext(sc, 2)
	spark=SparkSession(sc)
	#ssc.checkpoint("BIGDATA")
	record=ssc.socketTextStream("localhost",6100)
	
	
	def readMyStream(rdd):
	  if not rdd.isEmpty():
	    '''#df = spark.read.json(rdd)
	    #print('Started the Process')
	    #print('Selection of Columns')
	    #df = df.select('feature0','feature1','feature2')
	    #df=df.flatMap(lambda d:list(d[k] for k in d))
	    #df.show()'''
	    rdd1=rdd.map(lambda x: json.loads(x))
	    rdd2=rdd1.flatMap(lambda d:list(d[k] for k in d))
	    rdd3=rdd2.map(lambda x:tuple(x[k] for k in x))
	    #print(rdd3.take(3))
	    columns=['subject','message','class']
	    df=rdd3.toDF(columns)
	    df.printSchema()
	    df.show()
	    
	    
	record.foreachRDD( lambda rdd: readMyStream(rdd) )
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
