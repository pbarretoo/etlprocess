from lib2to3.pgen2 import token
from fastapi import Path
import pandas as pd
import xlrd
import dotenv
from dotenv import load_dotenv 
from datetime import datetime, timedelta
# df = 0
# for num in range(2000, 2021):
#     if num == 2000:
#         _ = pd.read_excel('vendasc.xls', sheet_name= f'p{num}')
#         df = _
#         # print(df)
    
#     else:
#         _ = pd.read_excel('vendasc.xls', sheet_name= f'p{num}')
#         # print(_)
#         df = pd.merge(df,_,how='outer')
# # print(df)

# df1 = df.rename(columns={'Jan':'01','Fev':'02','Mar':'03','Abr':'04','Mai':'05','Jun':'06','Jul':'07','Ago':'08','Set':'09','Out':'10','Nov':'11','Dez':'12'})

# df1_2 = df['COMBUSTÍVEL'].replace(['GASOLINA DE AVIAÇÃO (m3)','QUEROSENE ILUMINANTE (m3)','QUEROSENE DE AVIAÇÃO (m3)','ÓLEO DIESEL (m3)','ÓLEO COMBUSTÍVEL (m3)',\
#           'ETANOL HIDRATADO (m3)','GASOLINA C (m3)','GLP (m3)'],['GASOLINA DE AVIAÇÃO','QUEROSENE ILUMINANTE','QUEROSENE DE AVIAÇÃO','ÓLEO DIESEL','ÓLEO COMBUSTÍVEL','ETANOL HIDRATADO','GASOLINA C','GLP'])
# # print(df1_2)
# df1_tratado = df1.drop(columns=['COMBUSTÍVEL'])
# df = pd.concat([df1_2,df1_tratado],axis=1,join='inner')
# print(df)


# # print(df)

# df1_3 = df['ESTADO'].replace(['ACRE','ALAGOAS','AMAPÁ','AMAZONAS','BAHIA','CEARÁ','DISTRITO FEDERAL','ESPÍRITO SANTO','GOIÁS','MARANHÃO','MATO GROSSO','MATO GROSSO DO SUL',\
#     'MINAS GERAIS','PARÁ','PARAÍBA','PARANÁ','PERNAMBUCO','PIAUÍ','RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL','RONDÔNIA','RORAIMA','SANTA CATARINA','SÃO PAULO','SERGIPE','TOCANTINS'],\
#     ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'])

# # print(df1_3)
# df1_4 = df.drop(columns=['ESTADO'])
# df5 = pd.concat([df1_3,df1_4],axis=1,join='inner')
# # print(df5)

# colunas = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10','11', '12'] 
# valores = ['COMBUSTÍVEL', 'ANO', 'ESTADO', 'UNIDADE']
# df6 = df5.melt(id_vars = valores, value_vars = colunas, var_name='Mes', value_name='Valor')

# df7 = df6['ANO'].astype(str)+"/"+df6['Mes']
# df8 = df6.drop(columns=['ANO','Mes'])
# df9 = pd.concat([df8,df7], axis=1,join='inner')
# df9 = df9.rename(columns={0:'year_month','COMBUSTÍVEL':'product','UNIDADE':'unit','ESTADO':'uf','Valor':'volume'})
# print(df9)

# df9['year_month'] = pd.to_datetime(df9['year_month'], format="%Y/%m",errors='ignore')
# dt = datetime.now()

# ts = pd.to_datetime(dt, format="%m/%d/%Y, %H:%M:%S",errors='ignore')

# df9['created_at'] = ts

# df9 = df9.fillna(0)

# df9 = df9[['year_month','uf','product','unit','volume','created_at']]
# # df9


# df9 = df9.to_csv('sales_of_oil.csv',header=None, index=False, encoding='utf-8',sep=',')
# print(df9)
# my_file = open('sales_of_oil.csv')

import psycopg2
import os
import pandas as pd
import numpy as np

#### Creating the DB connection 
conn_string = [os.getenv('host'),os.getenv('dbname'),os.getenv('user'),os.getenv('password')]
conn_string = "host =pedrodb.ch5uojxrjo5o.us-east-1.rds.amazonaws.com \
                dbname='postgres' \
                user='pedro' \
                password='Password!'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()



##### Creating the table 
# cursor.execute("CREATE TABLE sales (year_month date, uf varchar(20), product varchar(30), unit varchar(5), volume double precision, created_at timestamp)")
# cursor.execute('commit')



##### Converting the csv into a new csv withou headers, open in memory

# my_file = open('sales_of_oil.csv',encoding='utf-8')
# # print(my_file)

# # ##### Creating a sql statement insert all dataset at once 
# SQL_STATEMENT = """
# COPY sales from STDIN WITH
#     CSV
#     HEADER
#     DELIMITER AS ','""" 
# cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
# cursor.execute('commit')



#### Testing if it's ok
cursor.execute("select * from sales_oil") 
for x in cursor:
    print(x)
