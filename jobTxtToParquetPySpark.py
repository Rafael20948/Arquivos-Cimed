#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from google.cloud import storage
from datetime import datetime

now = datetime.now()

#Jobs nomes ["Duplicatas", "Clientes"]

job="auditoria_brick"

#Dia da execuÃ§Ã£o do job

dia=now.strftime("_%d_%m_%Y")

lista_arquivos=[]

spark = SparkSession.builder.appName('TxtToParquet').getOrCreate()

#Dados Arquivo Origem

arq_origem="CEPBRICK*.txt"
dir_origem="gs://cimed-stage/dados_cimed_auditoria/"

#Dados Arquivo Destino

dir_destino="gs://cimed-stage/destino/carga"+dia+"/"

def le_arquivo():
    df = spark.read.load(dir_origem+arq_origem, format="text")
    return df

def trata_posicao():
    df1 = df.withColumn('brick', df.value.substr(1,9))\
            .withColumn('cidade_bairro',f.trim(df.value.substr(10,30)))\
            .withColumn('cep',f.trim(df.value.substr(40,8)))\
            .withColumn('cidade',f.trim(df.value.substr(48,30)))\
            .withColumn('estado',f.trim(df.value.substr(78,2)))\
            .withColumn('regiao',f.trim(df.value.substr(80,10)))\
            .drop('value')
    return df1

def grava_parquet():
    df1.write.parquet(dir_destino+job)

df = le_arquivo()     #Le arquivo txt
df1 = trata_posicao() #Trata o arquivo
grava_parquet()       #Gera arquivo parquet
