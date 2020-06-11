import os
import re
from ftplib import FTP
import ftplib
import zipfile
from ftplib import FTP_TLS
import threading
import signal
from datetime import datetime
#from concurrent.futures import TimeoutError
import socket
from contextlib import contextmanager


#        gs://cimed-stage/jobs-pyspark/sftp_to_gcs/Sftp_to_Gcs_v6.py


lista_timeouts=[]

now =datetime.now()
job="carga"
dia=now.strftime("_%d_%m_%Y")

carga=job+dia

def raise_timeout(signum, frame):
    raise TimeoutError
    
@contextmanager
def timeout(time,arquivo):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after ``time``.
    signal.alarm(time)

    try:
        yield
    except Exception as e:
        print("erro timeout",arquivo)
        lista_timeouts.append(arquivo)
        pass
    finally:
        # Unregister the signal so it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

def downloadFile(filename):
    with timeout(30,filename):

        ftp.set_debuglevel(2)
        sock = ftp.transfercmd('RETR ' + filename)
        def background():
            f = open("ftp/"+filename,"wb")
            while True:
                block = sock.recv(1024*1024)
                if not block:
                    break
                f.write(block)
                
            sock.close()
            print("Downloaded " + filename)
        t = threading.Thread(target=background)
        t.start()
        while t.is_alive():
            t.join(60)
            ftp.voidcmd('NOOP') 
            
def _download_ftp_file(ftp_handle, name, dest, overwrite):
    
    #print(ftp_handle)
    #print(name,dest,overwrite)
    #print(dest)
    """ downloads a single file from an ftp server """
    #_make_parent_dir(dest.lstrip("/"))
    if not os.path.exists(dest) or overwrite is True:
        try:
            #print("RETR {0}".format(name))
            with open("ftp/"+dest, 'wb') as f:
                try:
                    ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 75)
                    ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                    #ftp.set_debuglevel(2)
                    with timeout(20,dest):
                        
                        ftp_handle.retrbinary("RETR {0}".format(name), f.write)
                except Exception as e:
                    print("deu ruim aqui em retrbinary",e)
                #print(ftp_handle.set_debuglevel(2))
            print("downloaded: {0}".format(dest))
        except FileNotFoundError:
            print("FAILED: {0}".format(dest))
    else:
        print("already exists: {0}".format(dest))

def DownloadAllFiles_old(lista):
    #lista_timeouts=[]
    

    for i in lista:
        
        ftp =FTP(mysite)
        ftp.login(username,password)
        ftp.set_pasv(False)
        if i[0] =="/":



            _download_ftp_file(ftp,i[1:],i,True)


        else:


            _download_ftp_file(ftp,i,i,True)


def DownloadAllFiles(lista):
    #lista_timeouts=[]
    

    for i in lista:
        
        ftp =FTP(mysite)
        ftp.login(username,password)
        ftp.set_pasv(False)
        if i[0] =="/":



            downloadFile(i[1:])


        else:


            downloadFile(i)
            
            

        

if __name__ == "__main__":


    os.system("mkdir ftp/")
    os.system("mkdir ftp/historico/")
    os.system("mkdir ftp/geral/")
    os.system("mkdir ftp/duplicatas/")
    os.system("mkdir ftp/comissao/")

    mysite = "ftp.grupocimed.com.br"
    username = "leega"
    password = "lee2020!"
    ftp =FTP(mysite,timeout=240)
    ftp.connect(mysite,port=21)
    ftp.login(username,password)
    ftp.set_pasv(False)
    
    
    lista_tabela_faixa_comissao = ftp.nlst('/comissao/tabela_tx_comissao_frn_*.zip')  #arquivotabela_tx_comissao_frn_*.zip
    lista_tabela_precos_minimos=ftp.nlst("/comissao/tabela_zptl_*.zip") #envolve todos arquivos de preco minimo
    lista_tabela_auditoria_mercado= ftp.nlst("*.XLSX") #arquivo Estrutura XML X Base.XLSX
    lista_tabela_custo=ftp.nlst("/comissao/tabela_zfat_*.zip")#arquivo tabela_zfat_<n>.zip
    lista_tabela_duplicatas=ftp.nlst("/duplicatas/Duplicatas_*.zip") #arquivos Duplicatas_<n>.zip
    lista_tabela_clientes=ftp.nlst("/geral/Clientes_*") #arquivos Clientes_<n>.zip
    lista_tabela_cliente_representante=ftp.nlst("/geral/Cliente_Representante_*.zip") #arquivos Clientes_representando_<n>.zip
    lista_tabela_produto=ftp.nlst("/geral/Produtos_*") #arquivos Produto_<n>_*
    lista_tabela_historico_estoque= ftp.nlst("/historico/hist_estoque_*_*.zip") #arquivos hist_estoque_*_*.zip
    lista_tabela_historico_venda=ftp.nlst("/historico/hist_venda_*_*.zip") #arquivos hist_venda_*_*.zip
    lista_tabela_estrutura_produtos=ftp.nlst("/geral/Estrut_Produtos_*_*") #arquivos estrutura_produtos_*_*.zip

    lista_geral=lista_tabela_faixa_comissao+lista_tabela_precos_minimos+lista_tabela_auditoria_mercado+lista_tabela_custo+lista_tabela_duplicatas \
    +lista_tabela_clientes+lista_tabela_cliente_representante+ lista_tabela_produto + lista_tabela_historico_estoque + lista_tabela_historico_venda + lista_tabela_estrutura_produtos


    DownloadAllFiles(lista_geral)

    if len(lista_timeouts)>0:
        print("fazendo upload novamente dos arquivos que deu timeout")
        DownloadAllFiles(lista_timeouts)


    os.system("gsutil -m cp -r ftp gs://cimed-stage/Dados_cimed/{}".format(carga))



        

        
                           
                           
        
  