from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CovidAnalysis").master("local[2]").getOrCreate()

covidDF = spark.read.option("inferSchema", "true").csv("B:/Big data/Tableau/covid_19_data.csv", header=True)

formattedDF = covidDF.withColumn("Date_reported", psf.to_date("ObservationDate", "MM/dd/yyyy")).drop(
    "ObservationDate").cache()
formattedDF.show(truncate=False)

formattedDF.groupBy("Date_reported", "Country/Region").agg(psf.sum("Confirmed").alias("Total_affected_per_day")) \
    .orderBy(psf.col("Total_affected_per_day").desc())

formattedDF.filter("Date_reported > '2020-01-21' and Date_reported < '2020-03-23'") \
    .groupBy("Date_reported", "Country/Region").agg(psf.sum("Deaths").alias("Deaths/Country")) \
    .orderBy(psf.col("Deaths/Country").desc())

winFunc = Window.partitionBy("Country/Region")
winFunc2 = winFunc.orderBy("Total_confirmed")
formattedDF.withColumn("Total_confirmed", psf.last("Confirmed").over(winFunc)) \
    .withColumn("RowNum", psf.row_number().over(winFunc2)).filter("RowNum = 1") \
    .drop("RowNum").orderBy(psf.col("Total_confirmed").desc()).show(10, truncate=False)


@pandas_udf(BooleanType())
def myUDF(a):
    return len(a) != 0


formattedDF.filter(myUDF("Country/Region"))
