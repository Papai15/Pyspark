from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
data = ['cat bat hat', 'bat mat cat', 'bat mat that']


def load_RDD(Spark, Data):
    myRDD = Spark.sparkContext.parallelize(Data)
    return myRDD


def wordcount_func(myRDD):
    words = myRDD.flatMap(lambda x: x.split(' ')).map(lambda a: (a, 1)).reduceByKey(lambda x, y: x + y)
    return words.collect()


wordcount_func(load_RDD(spark, data))
