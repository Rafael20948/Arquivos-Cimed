#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext
import pandas as pd
from simpledbf import Dbf5
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from google.cloud import storage

#Inicia sessao spark
spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

storage_client = storage.Client()

bucket_name = "cimed-stage"

def download_blob(bucket_name, source_blob_name, destination_file_name):

        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

# Faz a leitura do arquivo no bucket
download_blob(bucket_name,"Dados cimed/OUTF970A.DBF", "OUTF970A.DBF")
# Le excel usando o pandas
dbf = Dbf5("OUTF970A.DBF")
# Cria um dataframe pandas
df = dbf.to_dataframe()
# Cria Schema da tabela
mySchema = StructType([ StructField("OUTLET_COD", StringType(), True)\
                       ,StructField("OUT_ID", StringType(), True)\
                       ,StructField("OUT_ASSOC", StringType(), True)\
                       ,StructField("OUT_NTIPO", StringType(), True)\
                       ,StructField("OUT_BAND", StringType(), True)\
                       ,StructField("OUT_GRUPO", StringType(), True)\
                       ,StructField("OUT_TPINF", StringType(), True)\
                       ,StructField("OUT_ATIVO", StringType(), True)\
                       ,StructField("OUT_FPOP", StringType(), True)\
                       ,StructField("OUT_CFRIA", StringType(), True)\
                       ,StructField("OUT_DTCAD", StringType(), True)\
                       ,StructField("OUT_DTALT", StringType(), True)\
                       ,StructField("OUT_CONT", StringType(), True)\
                       ,StructField("OUT_PBM", StringType(), True)\
                       ,StructField("OUT_STAT", StringType(), True)\
                       ,StructField("OUT_MESO", StringType(), True)\
                       ,StructField("OUT_MICRO", StringType(), True)\
                       ,StructField("OUT_MUNIC", StringType(), True)\
                       ,StructField("OUT_DELIV", StringType(), True)\
                       ,StructField("OUT_FICT", StringType(), True)])
# Cria dataframe pyspark com dados do pandas
spark_df = spark.createDataFrame(df,schema=mySchema)
# Escreve o conteudo do dataframe em um arquivo parquet
spark_df.write.parquet("gs://cimed-stage/convertidos/DBF/OUTF970A")

