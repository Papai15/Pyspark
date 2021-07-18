from pyspark.sql import SparkSession
from unittest import TestCase
from com.uday.pyspark.WordCount import load_RDD, wordcount_func, data


class UnitTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.appName("FirstTest").master("local[2]").getOrCreate()

    def TestWCFunc(self):
        rdd = load_RDD(self.spark, data)
        wc_result = wordcount_func(rdd)
        wc_map = dict()

        for k, v in wc_result:
            wc_map[k] = v

        self.assertEquals(wc_map['cat'], 2, 'test failed for cat')
        self.assertEquals(wc_map['mat'], 2, 'test failed with mat')
