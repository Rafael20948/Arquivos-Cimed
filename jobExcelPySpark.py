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
download_blob(bucket_name,"Dados cimed/estrutura-de-produto.xlsx", "estrutura-de-produto.xlsx")
# Le excel usando o pandas
excel = pd.read_excel("estrutura-de-produto.xlsx")
# Cria um dataframe pandas
df =  pd.DataFrame(data=excel)
# Cria Schema da tabela
mySchema = StructType([ StructField("Marc_Elim", StringType(), True)\
                       ,StructField("Prod_BU", StringType(), True)\
                       ,StructField("Prod_BU_Cod", StringType(), True)\
                       ,StructField("Prod_Chave", StringType(), True)\
                       ,StructField("Prod_Classe1", StringType(), True)\
                       ,StructField("Prod_Classe1_Cod", StringType(), True)\
                       ,StructField("Prod_Classe2", StringType(), True)\
                       ,StructField("Prod_Classe2_Cod", StringType(), True)\
                       ,StructField("Prod_Cod", StringType(), True)\
                       ,StructField("Prod_Cod_SAP", StringType(), True)\
                       ,StructField("Prod_CodCimed", StringType(), True)\
                       ,StructField("Prod_Custo", StringType(), True)\
                       ,StructField("Prod_EAN", StringType(), True)\
                       ,StructField("Prod_FormaFarm", StringType(), True)\
                       ,StructField("Prod_GP", StringType(), True)\
                       ,StructField("Prod_GP_Cod", StringType(), True)\
                       ,StructField("Prod_Lancamento", StringType(), True)\
                       ,StructField("Prod_Marca", StringType(), True)\
                       ,StructField("Prod_Molecula", StringType(), True)\
                       ,StructField("Prod_Nome", StringType(), True)\
                       ,StructField("Prod_Status", StringType(), True)\
                       ,StructField("Prod_Tipo", StringType(), True)])
# Cria dataframe pyspark com dados do pandas
spark_df = spark.createDataFrame(df,schema=mySchema)
# Escreve o conteudo do dataframe em um arquivo parquet
spark_df.write.parquet("gs://cimed-stage/convertidos/excel/estrutura-de-produtos")

