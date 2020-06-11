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

now =datetime.now()

#Inicia sessao spark
spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

storage_client = storage.Client()

#dir_arquivos_zip_destino recebe o valor onde será armazenado os arquivos zip extraido do bucket (na maquina local)
dir_arquivos_zip_destino="arquivos_zips/"

#nome_bucket recebe o valor do nome do bucket origem
nome_bucket="gs://cimed-stage/"

#source_object_in_bucket caminho do objeto(s)no bucket por exemplo gs://cimed-stage/Dados cimed/ -> Dados cimed/ é o nosso source object (OBS: essa variavel precisa de escape pq é usada no shell)
source_object_in_bucket="dados_cimed_auditoria"

#source_object_in_bucket_load caminho do objeto(s)no bucket por exemplo gs://cimed-stage/Dados cimed/ -> Dados cimed/ é o nosso source object (OBS: essa variavel não precisa do escape pq é usada para fazer load do parquet no bucket)

source_object_in_bucket_load="Dados cimed/"

destino_arquivos_xml="arquivos_xlsx/"

#destino_arquivo_parquet = "recebe o caminho do da pasta no bucket  onde será os arquivos armazenados xml"
destino_arquivo_parquet="destino/"

#job nome(arquivo(s) que está sendo transformado)
job="Mercado"

#Dia da execução do job
dia=now.strftime("_%d_%m_%Y")

subpasta=job+dia+"/"

def download_blob(source_blob_name, destination_file_name):
	print(source_blob_name)
	print(destination_file_name)
	storage_client = storage.Client()
	bucket = storage_client.bucket("cimed-stage")
	blob = bucket.blob(source_blob_name)
	blob.download_to_filename(destination_file_name)
		
def createSchema(file):

	# Faz a leitura do arquivo no bucket
	download_blob(source_object_in_bucket + file, file)
	print(os.path.getsize(file))
	excel = pd.read_excel(file)
	# Cria um dataframe pandas
	df =  pd.DataFrame(data=excel)
	# Cria Schema da tabela
	mySchema = StructType([ StructField("MARKET_ID", StringType(), True)\
						   ,StructField("MARKET_DESC", StringType(), True)\
						   ,StructField("MARKET_MASTER", StringType(), True)\
						   ,StructField("MARKET_TYPE", StringType(), True)\
						   ,StructField("DATABASE_ID", StringType(), True)\
						   ,StructField("LINE_MKT_ID", StringType(), True)\
						   ,StructField("LINE_MKT_DESC", StringType(), True)\
						   ,StructField("ATTRIBUTE_DESC", StringType(), True)\
						   ,StructField("ATTRIBUTE_VALUE", StringType(), True)\
						   ,StructField("PRODUCT_ID", StringType(), True)\
						   ,StructField("PRODUCT_DESC", StringType(), True)\
						   ,StructField("BRAND_ID", StringType(), True)\
						   ,StructField("BRAND_DESC", StringType(), True)\
						   ,StructField("LABORATORY_CODE", StringType(), True)\
						   ,StructField("LABORATORY_DESC", StringType(), True)\
						   ,StructField("CLASS_L4_ID", StringType(), True)\
						   ,StructField("NEC_CLASS_L4_ID", StringType(), True)\
						   ,StructField("FORM_L3_ID", StringType(), True)\
						   ,StructField("ETIC_FLAG_DESC", StringType(), True)\
						   ,StructField("GENERIC_FLAG_DESC", StringType(), True)\
						   ,StructField("MOLECULE_DESC", StringType(), True)\
						   ,StructField("PRODUCT_FACTOR", StringType(), True)])
	# Cria dataframe pyspark com dados do pandas
	spark_df = spark.createDataFrame(df,schema=mySchema)
	# Escreve o conteudo do dataframe em um arquivo parquet
	spark_df.write.parquet(nome_bucket+destino_arquivo_parquet+subpasta)

def main():
	os.system("mkdir destino")
	os.system("mkdir arquivos_zips")
	os.system("mkdir tmp")

	for i in storage_client.list_blobs("cimed-stage",prefix="Dados cimed/arquivos_xlsx/"):
		#print(str(i).split("/")[2].split(",")[0])
		nome_arquivo=str(i).split("/")[2].split(",")[0]
		if ".xlsx" in nome_arquivo:
			print (nome_arquivo)
			createSchema(nome_arquivo)

if __name__ == "__main__":
    main()
