# -*- coding: utf-8 -*-
"""
Created on Sun Mar  7 15:50:28 2021

@author: matheus.sartor
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel+Names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel+Graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
lessPopular = connections.sort(func.col("connections").asc()).first()

mostObscureName = names.filter(func.col("id") == lessPopular[0]).select("name").first()

print(mostObscureName[0] + " is the most Obscure superhero with " + str(lessPopular[1]) + " co-appearances.")

