import ftplib

# Variaveis FTP
filename = "Teste.txt" #Nome do arquivo
endereco = "ftp.grupocimed.com.br"
usuario = "leega"
senha = "lee2020!"

# Inicia conexao FTP
ftp = ftplib.FTP(endereco)
ftp.connect(endereco, port=21)
ftp.set_pasv(False)
ftp.login(usuario,senha)
#ftp.cwd("/sugestao") Este comando serve para indicar o caminho onde o arquivo sera guardado

myfile = open("/"+filename, "rb")
# Escreve arquivo no FTP
ftp.storlines("STOR "+filename, myfile)
ftp.quit()

"""
TODO
-Importar a biblioteca OS e criar lista de arquivos para guardar os modelos
-Fazer um for para armazenar cada um dos modelos da lista
"""
