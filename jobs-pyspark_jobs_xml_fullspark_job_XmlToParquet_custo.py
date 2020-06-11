
# coding=utf-8
#from google.cloud import storage
import os
import zipfile
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
from multiprocessing import Pool
import multiprocessing as mp



now =datetime.now()

#job nome(arquivo(s) que está sendo transformado)
job="custo"

#nome_arquivo, tendo a opção de usar * caso queira pegar todos arquivos com um formato especifico no bucket exemplo: "Clientes*.zip"
nome_arquivo="tabela_zfat_*.zip"

#Dia da execução do job
dia=now.strftime("_%d_%m_%Y")

subpasta=job+"/"

lista_arquivos=[]

spark = SparkSession.builder.appName('XmlToParquet').getOrCreate()

#no ambiente gcp(linux)
#os.system('gsutil cp gs://cimed-stage/Dados\ cimed/Clientes* ./archives_extracts') no ambiente gcp(linux)
#no ambiente windows

#dir_arquivos_zip_destino recebe o valor onde será armazenado os arquivos zip extraido do bucket (na maquina local)
dir_arquivos_zip_destino="arquivos_zips/"

#nome_bucket recebe o valor do nome do bucket origem
nome_bucket="gs://cimed-stage/"

pasta="carga"+dia+"/"+"comissao"+"/"

#source_object_in_bucket caminho do objeto(s)no bucket por exemplo gs://cimed-stage/Dados cimed/ -> Dados cimed/ é o nosso source object (OBS: essa variavel precisa de escape pq é usada no shell)
source_object_in_bucket="Dados_cimed/"+pasta

#source_object_in_bucket_load caminho do objeto(s)no bucket por exemplo gs://cimed-stage/Dados cimed/ -> Dados cimed/ é o nosso source object (OBS: essa variavel não precisa do escape pq é usada para fazer load do parquet no bucket)

source_object_in_bucket_load="Dados_cimed/"



destino_arquivos_xml="destino/xml/"+job+dia+"/"

#destino_arquivo_parquet = "recebe o caminho do da pasta no bucket  onde será os arquivos armazenados xml"
destino_arquivo_parquet="destino/carga"+dia+"/"

# <NewDataSet>
#   <duplicatas>
#     <codigo>0092512358</codigo>
#     <cliente>0001016309</cliente>
#     <pedido>0002398186</pedido>
#     <notafiscal>000050856</notafiscal>
#     <dataemissao>20190930</dataemissao>
#     <datavencimento>20200227</datavencimento>
#     <valororiginal>1804.53</valororiginal>
#     <duplicata>000050856-20D</duplicata>
#     <conta_matriz>0001016309</conta_matriz>
#     <linha_dig>00190000090219773200700221338171781780000180453</linha_dig>
#     <distribuidoraid>5</distribuidoraid>
#   </duplicatas>
#</NewDataSet>

#rootTag =  NewDataSet
#rowTag = duplicatas
#Tag principak xml


rootTag="NewDataSet"

#linha do xml
rowTag = "zfat"

# mySchema_duplicata = StructType([ StructField("cliente", IntegerType(), True) \
#                        ,StructField("codigo", IntegerType(), True) \
#                        ,StructField("conta_matriz", IntegerType(), True) \
#                        ,StructField("dataemissao", IntegerType(), True) \
#                        ,StructField("datavencimento", IntegerType(), True) \
#                        ,StructField("distribuidoraid", IntegerType(), True) \
#                        ,StructField("duplicata", StringType(), True) \
#                        ,StructField("linha_dig", StringType(), True) \
#                        ,StructField("notafiscal", IntegerType(), True) \
#                        ,StructField("pedido", IntegerType(), True)     
#                        ,StructField("valororiginal", DoubleType(), True)])




# mySchema_cliente = StructType([ StructField("aceita_falta", StringType(), True)\
#                        ,StructField("alvaradata", StringType(), True)\
#                        ,StructField("alvaradatasanit", StringType(), True)\
#                        ,StructField("alvaradatasivisa", StringType(), True)\
#                        ,StructField("alvaranumero", StringType(), True)\
#                        ,StructField("alvaranumerosanit", StringType(), True)\
#                        ,StructField("alvaranumerosivisa", StringType(), True)\
#                        ,StructField("ativo", StringType(), True)\
#                        ,StructField("bloqueio", StringType(), True)\
#                        ,StructField("caixa_fechada", StringType(), True)\
#                        ,StructField("cli_bai", StringType(), True)\
#                        ,StructField("cli_cep", StringType(), True)\
#                        ,StructField("cli_cgc", IntegerType(), True)\
#                        ,StructField("cli_cid", StringType(), True)\
#                        ,StructField("cli_cnt", StringType(), True)\
#                        ,StructField("cli_compl", StringType(), True)\
#                        ,StructField("cli_email", StringType(), True)\
#                        ,StructField("cli_end", StringType(), True)\
#                        ,StructField("cli_est", StringType(), True)\
#                        ,StructField("cli_ine", StringType(), True)\
#                        ,StructField("cli_limite", DoubleType(), True)\
#                        ,StructField("cli_num", StringType(), True)\
#                        ,StructField("cli_rzs", StringType(), True)\
#                        ,StructField("cli_tel", StringType(), True)\
#                        ,StructField("cli_tel2", StringType(), True)\
#                        ,StructField("codigo", IntegerType(), True)\
#                        ,StructField("condicaopg", StringType(), True)\
#                        ,StructField("conta_matriz", IntegerType(), True)\
#                        ,StructField("controlado", StringType(), True)\
#                        ,StructField("crossdocking", StringType(), True)\
#                        ,StructField("datacrf", StringType(), True)\
#                        ,StructField("distribuidoraid", IntegerType(), True)\
#                        ,StructField("filtro", StringType(), True)\
#                        ,StructField("flagdistribuidor", StringType(), True)\
#                        ,StructField("flagfarmacia", StringType(), True)\
#                        ,StructField("flagoutrativ", StringType(), True)\
#                        ,StructField("flagzonafranca", StringType(), True)\
#                        ,StructField("grupo_contas", StringType(), True)\
#                        ,StructField("nome_fantasia", StringType(), True)\
#                        ,StructField("resptecnicocrf", StringType(), True)\
#                        ,StructField("resptecniconome", StringType(), True)\
#                        ,StructField("status", StringType(), True)\
#                        ,StructField("tipocobranca", StringType(), True)\
#                        ,StructField("vnd_cod", StringType(), True)])

#*.zip indica que quero pegar todos Clientes que contem .zip

def ExtraiArquivosZipBucket():
    #exemplo do comando executado nessa função #'gsutil cp gs://cimed-stage/"Dados cimed"/Clientes* /archives_extracts'
    print("fazendo uploads dos arquivos {}{}{} {}".format(nome_bucket,source_object_in_bucket,nome_arquivo,dir_arquivos_zip_destino))
    os.system('gsutil -m cp {}{}{} {}'.format(nome_bucket,source_object_in_bucket,nome_arquivo,dir_arquivos_zip_destino))

def unzipFile(file):
    #faz o unzip do arquivo e armazena em um uma pasta chamada "tmp/" e guarda no bucket novamente
    print("unzipando arquivo:" + file)
    with zipfile.ZipFile(dir_arquivos_zip_destino+file, 'r') as zip_ref:
        zip_ref.extractall(r'tmp/')
    

      

    print("Download realizado do arquivo: "+file)
    
    return file


def read_file_xml():

    print("estou no read_file_xml")
    print(nome_bucket+source_object_in_bucket+destino_arquivos_xml+"*.xml")


  


    #le arquivo xml, transoforma em um dataframe spark
    print("lendo arquivo xml e transformando arquivo para parquet e guardando no diretorio: "+destino_arquivo_parquet)
    
    spark.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", rootTag) \
        .option("rowTag", rowTag) \
        .load(nome_bucket+destino_arquivos_xml+"*.xml").write.parquet(nome_bucket+destino_arquivo_parquet+subpasta)
                #gs://cimed-stage/Dados Cimed/arquivos_xml/Clientes_19_04_2020/*.xml      | gs://cimed-stage/Dados Cimed/destino/Clientes_19_04_2020/
    






        
if __name__ == "__main__":

    # execute only if run as a script

    os.system("mkdir destino")
    os.system("mkdir arquivos_zips")
    os.system("mkdir tmp")
  

    print("comecou o main")
    ExtraiArquivosZipBucket()
    print(os.listdir(dir_arquivos_zip_destino))

    l=os.listdir(dir_arquivos_zip_destino)
    print(mp.cpu_count())
    num_cpus=mp.cpu_count()
    pool = mp.Pool(processes=num_cpus)
    pool.map(unzipFile,l)
    print(os.listdir("tmp/"))

    #os.system("gsutil -m cp tmp/* {}{}{}{}".format(nome_bucket,source_object_in_bucket,destino_arquivos_xml,subpasta))
    os.system("gsutil -m cp tmp/* gs://cimed-stage/"+destino_arquivos_xml)
    read_file_xml()


    
 