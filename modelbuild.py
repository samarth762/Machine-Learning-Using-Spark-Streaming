import sys
import re
import json
import string
import sklearn
import pickle
from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.functions import lower, col
from pyspark.ml.feature import RegexTokenizer, CountVectorizer, PCA ,StopWordsRemover,StringIndexer
from sklearn.ensemble import RandomForestRegressor
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier,LogisticRegression,NaiveBayes,LinearSVC
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import confusion_matrix,classification_report
from sklearn import datasets
from sklearn.model_selection import train_test_split
import operator
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import classification_report
from sklearn.metrics import matthews_corrcoef
from sklearn.metrics import confusion_matrix
from sklearn.metrics import average_precision_score
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
from sklearn.metrics import f1_score, recall_score
import numpy as np
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.stem import PorterStemmer
#from nltk.tokenize import word_tokenize



#sqlContext = SQLContext(sc)

#model_inputs = sys.argv[1]


def main():
	sc = SparkContext(appName="test")
	ssc = StreamingContext(sc, 2)
	spark=SparkSession(sc)
	#ssc.checkpoint("BIGDATA")
	record=ssc.socketTextStream("localhost",6100)
	st=stopwords.words('english')
	spl='[@_!#$%^&*()<>?/\|}{~:]'
	
	
	def readMyStream(rdd):
		if not rdd.isEmpty():
			rdd1=rdd.map(lambda x: json.loads(x))
			rdd2=rdd1.flatMap(lambda d:list(d[k] for k in d))
			rdd3=rdd2.map(lambda x:tuple(x[k] for k in x))
			#print(rdd3.take(3))
			columns=['subject','message','class']
			df=rdd3.toDF(columns)
			
			#df.printSchema()
			#df.show()
			#df = df.replace(to_replace ='\n', value = '', regex = True)
			a=df.select('message').collect()
		    
			ps=PorterStemmer()
		    
			#df["transform"]=df.apply(preprosses,axis='message')
			#df.printSchema()
			#df.show()
			np.array(a)
			pre=[]
			# removing regrex from label column 
			df=df.withColumn('String_Label', F.regexp_replace('class', '\\W', ''))
			#removing null values
			df=df.filter(df.message !='')
			regexTokenizer = RegexTokenizer(inputCol="message", outputCol="tokenized", pattern="\\W")
			resultantdf=regexTokenizer.transform(df)
			# removal of stop word
			stopwordsRemover = StopWordsRemover(inputCol="tokenized", outputCol="filtered")
			resultantdf=stopwordsRemover.transform(resultantdf)
			#resultantdf.show()
			
			
			
			
			

			
			
			
			
			
			
			cv = CountVectorizer(inputCol="filtered", outputCol="features")
			model=cv.fit(resultantdf)
			result=model.transform(resultantdf)
			#resultantdf.show()
			#print(type(result),"type")
			#result.show()
			result=result.drop('tokenized').drop('filtered')
			result=result.drop('class')
			indexer = StringIndexer(inputCol="String_Label", outputCol="label")
			indexed = indexer.fit(result).transform(result)
			indexed.select("features").show()
			
			
			x_f=indexed.select("features").collect()
			x_l=indexed.select("label").collect()
			
			svm= SGDClassifier(loss='hinge', penalty='l1', l1_ratio=1)
			svm.partial_fit(x_f,x_l,[0.0,1.0])
			#pickle.dump()
			
			
			#trainingData.show()
			#testData.show()
			
			
			
			
		
		
		    
	    
	record.foreachRDD( lambda rdd: readMyStream(rdd) )
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
