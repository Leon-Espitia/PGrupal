#!/usr/bin/python
import pandas as pd
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = (
  SparkSession
  .builder
  .master('yarn')
  .appName('bigquery-analytics-varsstdp')
  .getOrCreate()
)

bucket = 'spark-bucket-987'

spark.conf.set('temporaryGcsBucket', bucket)

df = spark.read.json('gs://testingnuevo/business.json')

df = df.drop('attributes')

df = df.drop('hours')

df = df.toPandas()

df = df.dropna(subset=["categories"])


business = df[df.categories.str.contains('Restaurants')]


# Spark saving
(
  business.format('bigquery')
  .option('table', 'proyecto-grupal-369315.Henry.business')
  .mode('append')
  .save()
)