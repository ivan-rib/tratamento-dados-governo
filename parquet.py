from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as B
from pyspark.sql.functions import avg, max, min, col, count,regexp_replace, sum
from datetime import datetime
import pandas as pd
import math
import logging
import os

spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
	

dados_parquet = spark.read.parquet(r"C:\spark\atividade_cluster")

calculos = dados_parquet.groupBy('NOME')\
    .agg(B.max("REM_LIQ")\
    , B.min("REM_LIQ")\
    , B.avg("REM_LIQ")\
    , B.sum("REM_LIQ")\
    ,B.count("REM_LIQ")).collect()

calculo1 = pd.DataFrame(calculos)
calculo1.columns = ['nome', 'max','min', 'media', 'soma', 'count']
print(calculo1)

