# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 16:06:43 2021

@author: matheus.sartor
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Total Amount By Customer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    itemPrice = fields[2]
    return (int(customerID), float(itemPrice))

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
priceAmount = parsedLines.reduceByKey(lambda x,y : x + y)
priceAmountSorted = priceAmount.map(lambda x: (x[1], x[0])).sortByKey()
results = priceAmountSorted.collect();

for result in results:
    print(str(result[0]) + "- R$ " + str(round(result[1],2)))