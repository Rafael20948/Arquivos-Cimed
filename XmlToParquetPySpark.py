#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from google.cloud import storage
import xml.etree.ElementTree as ET

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
download_blob(bucket_name,"Dados cimed/Estoque_1005.xml", "Estoque_1005.xml")
# Le excel usando o pandas
tree = ET.parse("Estoque_1005.xml")
root = tree.getroot()

get_range = lambda col: range(len(col))
l = [{r[i].tag:r[i].text for i in get_range(r)} for r in root]

# Cria um dataframe pandas
df =  df = pd.DataFrame.from_dict(l)
# Cria Schema da tabela
mySchema = StructType([ StructField("Prod_cod", StringType(), True)\
                       ,StructField("Qtde", StringType(), True)\
                       ,StructField("Transito", StringType(), True)\
                       ,StructField("Distribuidoraid", StringType(), True)])
# Cria dataframe pyspark com dados do pandas
spark_df = spark.createDataFrame(df,schema=mySchema)
# Escreve o conteudo do dataframe em um arquivo parquet
spark_df.write.parquet("gs://cimed-stage/convertidos/xml/Estoque_1005")
