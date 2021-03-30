# -*- coding: utf-8 -*-
"""
Created on Sat Mar  6 20:53:25 2021

@author: matheus.sartor
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalByCustomer").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("Price", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
custPrice = df.select("customerId","Price")
PriceAmount = custPrice.groupBy("customerId").agg((func.round(func.sum("Price"),2)).alias("totalSpent")).sort("totalSpent")
PriceAmount.show(PriceAmount.count())

spark.stop()