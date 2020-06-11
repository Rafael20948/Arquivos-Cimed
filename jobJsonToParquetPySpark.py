# coding=utf-8


import os
import multiprocessing as mp
import zipfile
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import json
from datetime import datetime

now = datetime.today()
dia = now.strftime("_%d_%m_%Y")


def unzipFile(file):
    with zipfile.ZipFile("arquivos/"+file, 'r') as zip_ref: #Le arquivos no diretorio e extrai um a um para a pasta extraidos
        zip_ref.extractall(r'tmp/')

def criaNovoJson(file):

    with open("tmp/"+file, 'r') as j:
        contents = json.loads(j.read())

    with open ('novo_json/'+file,"w")as new_file:
        json.dump(contents['T_DADOS'],new_file)

def particionandoArquivosJson(file):

    #os.system("/"+file.replace(".json",""))
    with open("novo_json/"+file,'r') as infile:
        o = json.load(infile)
        chunkSize = 4550

        for i in range(0, len(o), chunkSize):
            
            try:
                with open("arquivos_particionados/{}".format(file) + '_' + str(i//chunkSize) + '.json', 'w') as outfile:
                    json.dump(o[i:i+chunkSize], outfile)
                        #arquivos_particionados/hist_estoque_2018_03.json/hist_estoque_2018_03.json_<n>.json

            except:
                print("deu erro")
                
if __name__ == "__main__":
    
    #Cria pastas dentro da maquina dataproc
    os.system("mkdir arquivos")
    #os.system("mkdir extraidos")
    os.system("mkdir novo_json")
    os.system("mkdir arquivos_particionados")
    print("Pastas criadas")
    #Copia arquivo do bucket para a pasta criada
    #caminho = "gsutil -m cp gs://cimed-stage/Dados_cimed/historico/Estrut_produtos*.zip arquivos"
    os.system("gsutil -m cp gs://cimed-stage/Dados_cimed/carga"+dia+"/historico/hist_estoque*.zip arquivos")
    print("Arquivos copiados do bucket")
    #Cria uma lista com todos os arquivos na pasta
    lista_arquivo = os.listdir("arquivos")
    print("Lista de arquivos criadas")
    #Printa todos os arquivos na lista
    print(lista_arquivo)

    #Inicia processamento paralelo atraves do multiprocessing
    cores = mp.cpu_count() #Numero de cores do CPU
    pool = mp.Pool(processes=cores) #Indica que o mp deve usar todos os cores
    pool.map(unzipFile,lista_arquivo) #Executa funcao unzipFile em paralelo, passando por parametro a lista de arquivos "lista_arquivo" criada anteriormente
    print("Processamento paralelo")
    # Cria nova estrutura json
    json_extraidos = os.listdir("tmp/")
    pool.map(criaNovoJson,json_extraidos)
    print("Cria novo json")
    # Particiona Json
    json_novos = os.listdir("novo_json/")
    print("Conteudo da pasta novo_json ", json_novos)
    pool.map(particionandoArquivosJson, json_novos)
    print("Particiona json")
    # Move json particionados para o bucket
    os.system("gsutil -m cp -r arquivos_particionados/ gs://cimed-stage/destino/arquivos_json/historico_estoque")
    print("Copia json particionados")
    # Leitura dos dados gravados no bucket no dataframe spark
    spark = SparkSession.builder.appName('XmlToParquet').getOrCreate()
    sc=spark.sparkContext
    sqlContext = pyspark.SQLContext(sc)
    
    rdd = sc.textFile("gs://cimed-stage/destino/arquivos_json/historico_estoque/*.json")
    df = sqlContext.read.json(rdd)
   
    df.write.parquet("gs://cimed-stage/destino/carga"+dia+"/historico_estoque")
    







    
