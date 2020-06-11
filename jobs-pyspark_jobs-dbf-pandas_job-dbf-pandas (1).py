#!/usr/bin/python
# -- coding: utf-8 --
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import datetime
from simpledbf import Dbf5

now = datetime.now()

#Inicia sessao spark
spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
storage_client = storage.Client()

#nome_bucket recebe o valor do nome do bucket origem
nome_bucket="gs://cimed-stage/"

#source_object_in_bucket caminho do objeto(s)no bucket por exemplo gs://cimed-stage/Dados cimed/ -> Dados cimed/ é o nosso source object (OBS: essa variavel precisa de escape pq é usada no shell)
source_object_in_bucket="dados_cimed_auditoria/"

#Nome arquivos dbfs
arquivos = {"OUTF": "auditoria_outf", "OUTL":"auditoria_outl","PACK": "auditoria_pack", "PFLG": "auditoria_pflg","PROD": "auditoria_prod" , "RATE" : "auditoria_rate"}

#Dia da execução do job
dia=now.strftime("_%d_%m_%Y")
dir_destino = "carga" + dia + "/"

def download_blob(source_blob_name, destination_file_name):
	storage_client = storage.Client()
	bucket = storage_client.bucket("cimed-stage")
	blob = bucket.blob(source_blob_name)
	blob.download_to_filename(destination_file_name)
		
def createSchema(file):
	# Faz a leitura do arquivo no bucket
	download_blob(source_object_in_bucket + file, file)
	dbf = Dbf5(file)
	df = dbf.to_dataframe()
	all_columns = list(df)
	df[all_columns] = df[all_columns].astype(str)

	# Cria dataframe pyspark com dados do pandas
	spark_df = spark.createDataFrame(df)
	
	# Trata nome ppra pasta destino
	nome_arquivo = arquivos[file[0:4]]

	# Escreve o conteudo do dataframe em um arquivo parquet
	spark_df.write.parquet(nome_bucket+"destino/"+ dir_destino + nome_arquivo)

def main():
	for i in storage_client.list_blobs("cimed-stage",prefix="dados_cimed_auditoria"):
		nome_arquivo=str(i).split("/")[1].split(",")[0]
		if ".DBF" in nome_arquivo:
			createSchema(nome_arquivo)

if __name__ == "__main__":
    main()