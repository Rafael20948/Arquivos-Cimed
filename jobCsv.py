#!/usr/bin/python
import pyspark

sqlContext = pyspark.SQLContext(pyspark.SparkContext())
df = sqlContext.read.csv("gs://cimed-stage/Dados cimed/CloseUP - Potencial.csv")
df.write.parquet("gs://cimed-stage/convertidos/csv/closeup")
