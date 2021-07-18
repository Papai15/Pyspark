from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

spark = SparkSession.builder.appName("check_counts").master("local[2]").getOrCreate()

myRDD = spark.sparkContext.textFile("C:/Users/uday/OneDrive/Desktop/data.txt")

myRDD2 = myRDD.map(lambda x: (x.split(" ")[0], x.split(" ")[1], x.split(" ")[2]))
myDF = spark.createDataFrame(myRDD2, ["col1", "col2", "col3"])

myDF.show()

DF1 = myDF.groupBy("col1").count().alias("col_count").withColumn("category", psf.lit("id"))
DF2 = myDF.groupBy("col2").count().alias("col_count").withColumn("category", psf.lit("type"))
DF3 = myDF.groupBy("col3").count().alias("col_count").withColumn("category", psf.lit("status"))

DF1.union(DF2).union(DF3).show()
