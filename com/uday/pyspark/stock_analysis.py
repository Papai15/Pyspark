import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("stock_analysis") \
        .master("local[2]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    input_dir = sys.argv[1]
    index_file = "indexData.csv"
    info_file = "indexInfo.csv"
    processed_file = "indexProcessed.csv"

    index_schema = StructType([
        StructField("Index", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("Adj Close", DoubleType(), False),
        StructField("Volume", DoubleType(), False)
    ])

    info_schema = StructType([
        StructField("Region", StringType(), True),
        StructField("Exchange", StringType(), True),
        StructField("Index", StringType(), True),
        StructField("Currency", StringType(), True)
    ])


    def load_DFs(Spark, path, file, schema):
        return Spark.read.option("inferSchema", "true") \
            .schema(schema) \
            .csv(path + "\\" + file, header=True)


    index_df = load_DFs(spark, input_dir, index_file, index_schema)
    info_df = load_DFs(spark, input_dir, info_file, info_schema)
    processed_df = load_DFs(spark, input_dir, processed_file, index_schema.add("CloseUSD", DoubleType(), False))

    dfs = [index_df, info_df, processed_df]

    # for df in dfs:
    #    df.show(10, truncate=False)

    index_info = index_df.join(info_df, ["Index"]).cache()

    # based on currency top 5 opening price
    index_info.groupBy("Currency") \
        .agg(f.round(f.max("Open"), 2).alias("highest_opening")) \
        .orderBy(f.col("highest_opening").desc()) \
        .show(5, truncate=False)

    max_date = index_info.selectExpr("max(Date) as max_date").collect()[0][0]

    # 10 stock exchanges based on highest value and minimum lowest in USA in last 10 years
    index_info.filter((f.col("Region") == "United States") & (f.col("Date") >= f.add_months(f.lit(max_date), -120))) \
        .groupBy(f.year(f.col("Date")).alias("year")).agg({"High": "max", "Low": "min"}) \
        .orderBy("year") \
        .show(10, truncate=False)

    index_processed = index_df.join(processed_df, ["Index"], "leftsemi").cache()

    win_func = Window.partitionBy(f.col("Region"), f.col("Exchange")) \
        .orderBy(f.col("price_gap").desc())

    # top 10 stock exchanges price_gap between highest & lowest which have been processed
    index_processed.withColumn("price_gap", f.round((f.col("High") - f.col("Low")), 2)) \
        .join(broadcast(info_df), ["Index"]) \
        .withColumn("rank", dense_rank().over(win_func)) \
        .filter(f.col("rank") == 1).drop("rank") \
        .select("Region", "Exchange", "price_gap") \
        .orderBy(f.col("price_gap").desc()) \
        .show(10, truncate=False)
