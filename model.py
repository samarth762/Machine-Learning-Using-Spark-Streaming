import sys
import re
import json
import string
from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.functions import lower, col
from pyspark.ml.feature import RegexTokenizer, CountVectorizer, PCA ,StopWordsRemover,StringIndexer
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier,LogisticRegression,NaiveBayes,LinearSVC
import operator
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
			
			(trainingData, testData) = indexed.randomSplit([0.7, 0.3], seed = 100)
			#trainingData.show()
			#testData.show()
			
			
			rf = RandomForestClassifier(labelCol="label",featuresCol="features",numTrees = 30,maxDepth = 20)
			# Train model with Training Data
			rfModel = rf.fit(trainingData)
			# Prediction
			predictions = rfModel.transform(testData)
			predictions.select("prediction").show()
			print("pred")
			testData.select("label").show()
			print("label")
		
			evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
			acc1=evaluator.evaluate(predictions)
			print(acc1,"rf")
			
			
			
			
			
			lsvc = LinearSVC(maxIter=10, regParam=0.1)
			# Fit the model
			lsvcModel = lsvc.fit(trainingData)
			predictions = lsvcModel.transform(testData)
			predictions.select("prediction").show()
			print("pred")
			testData.select("label").show()
			print("label")
		
			evaluatorsvm = MulticlassClassificationEvaluator(predictionCol="prediction")
			acc2=evaluatorsvm.evaluate(predictions)
			print(acc2,"svm")
			
			nb = NaiveBayes(smoothing=1)
			# train the model
			model = nb.fit(trainingData)
			# prediction
			predictions= model.transform(testData)
			predictions.select("prediction").show()
			print("pred")
			testData.select("label").show()
			print("label")
		
			evaluatorlr = MulticlassClassificationEvaluator(predictionCol="prediction")
			acc3=evaluatorlr.evaluate(predictions)
			print(acc3,"nb")
			"""#def preprosses(a):
				#up_l=[]
				#print("hello")
				print(a)
				up_l=[]
				#a=a.lower()
				a=a.split()
				for j in a:
					#j=j.lower()		    		
					
					for l in j:
							
						#print(l,type(l))
						if (l not in st) and (l.isalpha()) and (l not in spl) and (len(l)>2):
							
							print(l)
							a=ps.stem(l)
							print(a)
							up_l.append(a)
			for i in a:
				#print(i)
				up_l=[]
				for j in i:
					j=j.lower()
					j=j.split()
					#print(j)
					for l in j:
						
						#print(l,type(l))
						if (l not in st) and (l.isalpha()) and (l not in spl) and (len(l)>2):
						
							#print(l)
							a=ps.stem(l)
							#print(a)
							up_l.append(a)
				line=" ".join(up_l)
				#newDf = df.withColumn("pre", when(col("message")== i, line).otherwise(1))
				pre.append(tuple(str(line)))
			
				
				#print(line)
			print(pre)
			#dfp = spark.createDataFrame(pre).toDF("prep")
			#dfp.printSchema()
			#dfp.show()
				#words = line.split(" ")
				#rdd10 = spark.sparkContext.parallelize(words)
				#wordCounts = rdd10.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
				#print(wordCounts)
			#preprosses("a hello how what 1 2 33 arcgyt")
			#udf_star_desc = F.udf(f=lambda x:preprosses(x),returnType=T.StringType() )
			#df=df.withColumn("preprossesed",udf_star_desc(df.message))
			#df=df.withColumn("hhh",preprosses(("message")))
			
			#df.printSchema()
			#df.show()"""
			
		
		
		    
	    
	record.foreachRDD( lambda rdd: readMyStream(rdd) )
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
