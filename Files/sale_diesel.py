import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator



def transform(**kwargs):
    df = pd.read_excel('/home/pedro/Documents/Untitled 1.ods',sheet_name='DPCache_m3_2')
    df = df.drop(columns=['TOTAL'])

    # Rename month 
    df1 = df.rename(columns={'Jan':'01','Fev':'02','Mar':'03','Abr':'04','Mai':'05','Jun':'06','Jul':'07','Ago':'08','Set':'09','Out':'10','Nov':'11','Dez':'12'})


    # Rename fuel names
    df1_2 = df['COMBUSTÍVEL'].replace(['ÓLEO DIESEL (OUTROS ) (m3)','ÓLEO DIESEL MARÍTIMO (m3)','ÓLEO DIESEL S-10 (m3)','ÓLEO DIESEL S-1800 (m3)','ÓLEO DIESEL S-500 (m3)'],\
        ['OLEO DIESEL(OUTROS)','OLEO DIESEL MARITIMO','OLEO DIESEL S-10','OLEO DIESEL S-1800','OLEO DIESEL S-500'])
    df1_tratado = df1.drop(columns=['COMBUSTÍVEL'])
    df = pd.concat([df1_2,df1_tratado],axis=1,join='inner')
    # print(df)

    # Rename UF's
    df1_3 = df['ESTADO'].replace(['ACRE','ALAGOAS','AMAPÁ','AMAZONAS','BAHIA','CEARÁ','DISTRITO FEDERAL','ESPÍRITO SANTO','GOIÁS','MARANHÃO','MATO GROSSO','MATO GROSSO DO SUL',\
        'MINAS GERAIS','PARÁ','PARAÍBA','PARANÁ','PERNAMBUCO','PIAUÍ','RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL','RONDÔNIA','RORAIMA','SANTA CATARINA','SÃO PAULO','SERGIPE','TOCANTINS'],\
        ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'])

    # print(df1_3)
    df1_4 = df.drop(columns=['ESTADO'])
    df5 = pd.concat([df1_3,df1_4],axis=1,join='inner')


    # Putting month and year in the same column and renaming the column
    colunas = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10','11', '12'] 
    valores = ['COMBUSTÍVEL', 'ANO', 'ESTADO']
    df21 = df5.melt(id_vars = valores, value_vars = colunas, var_name='Mes', value_name='Valor')
    df22 = df21['ANO'].astype(str)+"/"+df21['Mes']
    df23 = df21.drop(columns=['ANO','Mes'])
    df24 = pd.concat([df23,df22], axis=1,join='inner')

    df24 = df24.rename(columns={0:'year_month','COMBUSTÍVEL':'product','ESTADO':'uf','Valor':'volume'})


    # Converting the datatype to datetime 
    df24['year_month'] = pd.to_datetime(df24['year_month'], format="%Y/%m",errors='ignore')

    # Creating the timestamp column
    dt = datetime.now()
    ts = pd.to_datetime(dt, format="%m/%d/%Y, %H:%M:%S",errors='ignore')
    df24['created_at'] = ts

    # Filling the NaN with 0
    df24 = df24.fillna(0)

    # Creating the unit column
    df24['unit'] = 'm3'

    # Reordering the ds
    df24 = df24[['year_month','uf','product','unit','volume','created_at']]

    # Saving the ds into a csv file
    df24 = df24.to_csv('/home/pedro/Documents/sales_of_diesel12.csv',header=None, index=False, encoding='utf-8',sep=',')
    

# transform()

def ingest1(**kwargs):
#### Creating the DB connection 
    conn_string = "host =rds.amazonaws.com \
                    dbname='postgres' \
                    user='xxxxx' \
                    password='xxxx'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()


##### Creating the table 
# cursor.execute("CREATE TABLE sales_oil (year_month date, uf varchar(20), product varchar(30), unit varchar(5), volume double precision, created_at timestamp)")
# cursor.execute('commit')



##### Converting the csv into a new csv withou headers, open in memory

    my_file = open('/home/pedro/Documents/sales_of_diesel12.csv',encoding='utf-8')
    print(my_file)

    ##### Creating a sql statement insert all dataset at once 
    SQL_STATEMENT = """
    COPY sales from STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','""" 
    cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
    cursor.execute('commit')
# ingest()


with DAG('SalesOfDiesel', start_date=datetime(2022,7,15),
          schedule_interval='@monthly',catchup=False) as dag:  
    
    modeling = PythonOperator(
        task_id="modeling"
        ,python_callable=transform)
        

    ingesting = PythonOperator(
        task_id='ingesting'
        ,python_callable=ingest1)


modeling>>ingesting
