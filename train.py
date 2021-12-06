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
			
			
			#trainingData.show()
			#testData.show()
			
			
			
			
			"""# Number of trees in random forest
			n_estimators = [int(x) for x in np.linspace(start = 200, stop = 2000, num = 10)]
			# Number of features to consider at every split
			max_features = ['auto', 'sqrt']
			# Maximum number of levels in tree
			max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
			max_depth.append(None)
			# Minimum number of samples required to split a node
			min_samples_split = [2, 5, 10]
			# Minimum number of samples required at each leaf node
			min_samples_leaf = [1, 2, 4]
			# Method of selecting samples for training each tree
			bootstrap = [True, False]
			# Create the random grid
			x_f=trainingData.select("features").collect()
			x_l=trainingData.select("label").collect()
			y_f=testData.select("features").collect()
			y_l=testData.select("label").collect()
			random_grid = {'n_estimators': n_estimators,
				       'max_features': max_features,
				       'max_depth': max_depth,
				       'min_samples_split': min_samples_split,
				       'min_samples_leaf': min_samples_leaf,
				       'bootstrap': bootstrap}
			print(type(random_grid),"rg")
			
			# Use the random grid to search for best hyperparameters
			# First create the base model to tune
			rf = RandomForestRegressor()
			# Random search of parameters, using 3 fold cross validation, 
			# search across 100 different combinations, and use all available cores
			rf_random = RandomizedSearchCV(estimator = rf, param_distributions = random_grid, n_iter = 100, cv = 3, verbose=2, random_state=42, n_jobs = -1)
			# Fit the random search model
			rf_random.fit(x_f, x_l)
			rf_random.best_params_
			def evaluate(model, y_f, y_l):
			    predictions = model.predict(y_f)
			    errors = abs(predictions - y_l)
			    mape = 100 * np.mean(errors /y_l)
			    accuracy = 100 - mape
			    print('Model Performance')
			    print('Average Error: {:0.4f} degrees.'.format(np.mean(errors)))
			    print('Accuracy = {:0.2f}%.'.format(accuracy))
			    
			    return accuracy
			base_model = RandomForestRegressor(n_estimators = 10, random_state = 42)
			base_model.fit(x_f, f_l)
			base_accuracy = evaluate(base_model, y_f, y_l)
			
			best_random = rf_random.best_estimator_
			random_accuracy = evaluate(best_random, y_f, y_l)
			
			print('Improvement of {:0.2f}%.'.format( 100 * (random_accuracy - base_accuracy) / base_accuracy))
			"""
			
			
			rf = RandomForestClassifier(labelCol="label",featuresCol="features",numTrees = 30,maxDepth = 20)
			# Train model with Training Data
			rfModel = rf.fit(indexed)
			# Prediction

			# save the model to disk
			pickle.dump(rfModel,open("rf_m","wb"))
			"""with open("model_1","wb") as files:
				pickle.dump(rfModel,files)"""
			
			print ("Trying to fit the Random Forest model --> ")
			if os.path.exists('rf.pkl'):
			    print ("Trained model already pickled -- >")
			    with open('rf.pkl', 'rb') as f:
				rf = cPickle.load(f)
			else:
			    df_x_train = x_train[col_feature]
			    rf.fit(df_x_train,y_train)
			    print ("Training for the model done ")
			    with open('rf.pkl', 'wb') as f:
				cPickle.dump(rf, f)
			df_x_test = x_test[col_feature]
			pred = rf.predict(df_x_test)
			
			
			
			
			
			"""
			lsvc = LinearSVC(maxIter=10, regParam=0.1)
			# Fit the model
			lsvcModel = lsvc.fit(indexed)
			
			# save the model to disk
			filename = 'finalized_modelsvm.sav'
			pickle.dump(lsvcModel, open(filename, 'wb'))
			
			
			
			
			nb = NaiveBayes(smoothing=1)
			# train the model
			model = nb.fit(indexed)
			# prediction
			# save the model to disk
			filename = 'finalized_modelnb.sav'
			pickle.dump(lsvcModel, open(filename, 'wb'))
			#predictions.select("prediction").show()
			#print("pred")
			#testData.select("label").show()
			#print("label")
		
		
		
		
		
		
			#def preprosses(a):
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
