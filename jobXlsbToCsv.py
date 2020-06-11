from pyxlsb import open_workbook
import pandas as pd
import os 

df = []

os.system('gsutil -m cp "gs://cimed-stage/dados_cimed_auditoria/Dados cimed_Dados cimed_CloseUP - Potencial.xlsb" .')
print('Enviando arquivo para maquina dataproc')
with open_workbook("Dados cimed_Dados cimed_CloseUP - Potencial.xlsb") as wb:
    for sheetname in wb.sheets:
        with wb.get_sheet(sheetname) as sheet:
            for row in sheet.rows():
                df.append([item.v for item in row])
print('Lendo arquivo xlsb')
df = pd.DataFrame(df[1:], columns=df[0], dtype="string")
print('Copiado no dataframe')
os.system('mkdir tmp')
print('Copiando na pasta tmp')
df.to_csv ('tmp/auditoria_potencial_venda.csv', encoding='utf-8', header=True)
print('Convertendo para csv')
os.system('gsutil -m cp -r tmp/auditoria_potencial_venda.csv gs://cimed-stage/dados_cimed_auditoria/auditoria_potencial_venda.csv')
print('TERMINO')
