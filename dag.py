from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from urllib import request

import subprocess
from datetime import datetime

import pandas as pd
import numpy as np

def xlsx():
  url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
  response = request.urlretrieve(url, 'vendas-combustiveis-m3.xls')

def create_df():
  df_s1 = pd.read_excel('converted/vendas-combustiveis-m3.xlsx', sheet_name=1)
  df_s2 = pd.read_excel('converted/vendas-combustiveis-m3.xlsx', sheet_name=2)
  df_s3 = pd.read_excel('converted/vendas-combustiveis-m3.xlsx', sheet_name=3)
  frames = [df_s1, df_s2, df_s3]
  df = pd.concat(frames)
  return df

def alter_table(df):
  df.columns = ['combustivel', 'ano', 'regiao', 'estado', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'total']
  df['product'] = df['combustivel'].map(lambda x: x.rstrip('(m3)'))
  df['unit'] = 'm3'
  df.drop(['combustivel', 'regiao','total'],axis=1 ,inplace=True)
  lista_estados = ['RONDÔNIA', 'ACRE', 'AMAZONAS', 'RORAIMA', 'PARÁ', 'AMAPÁ', 'TOCANTINS', 'MARANHÃO', 'PIAUÍ', 'CEARÁ', 'RIO GRANDE DO NORTE',
                   'PARAÍBA', 'PERNAMBUCO', 'ALAGOAS', 'SERGIPE', 'BAHIA', 'MINAS GERAIS', 'ESPÍRITO SANTO', 'RIO DE JANEIRO', 'SÃO PAULO',
                   'PARANÁ', 'SANTA CATARINA', 'RIO GRANDE DO SUL', 'MATO GROSSO DO SUL', 'MATO GROSSO', 'GOIÁS', 'DISTRITO FEDERAL']
  lista_uf = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP',
              'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF']
  df.estado.replace(lista_estados, lista_uf, inplace=True)
  lista_combustiveis = ['GASOLINA C ', 'GASOLINA DE AVIAÇÃO ', 'QUEROSENE ILUMINANTE ', 'QUEROSENE DE AVIAÇÃO ', 'ÓLEO DIESEL ', 'ÓLEO COMBUSTÍVEL ',
                        'ETANOL HIDRATADO ', 'GLP ', 'ÓLEO DIESEL S-10 ', 'ÓLEO DIESEL S-500 ', 'ÓLEO DIESEL S-1800 ', 'ÓLEO DIESEL MARÍTIMO ',
                        'ÓLEO DIESEL (OUTROS ) ', 'GLP - Até P13 ', 'GLP - Outros ']

  lista_combustiveis_f = ['GASOLINA C', 'GASOLINA DE AVIACAO', 'QUEROSENE ILUMINANTE', 'QUEROSENE DE AVIACAO', 'OLEO DIESEL', 'OLEO COMBUSTÍVEL',
                          'ETANOL HIDRATADO', 'GLP', 'OLEO DIESEL S-10', 'OLEO DIESEL S-500', 'OLEO DIESEL S-1800', 'OLEO DIESEL MARITIMO',
                          'OLEO DIESEL - OUTROS', 'GLP - ATE P13', 'GLP - OUTROS']
  df['product'].replace(lista_combustiveis, lista_combustiveis_f, inplace=True)
  df = df.melt(id_vars=['product', 'ano', 'estado', 'unit'])
  df['year_month'] = df['ano'].astype(str) + '-' + df['variable'].astype(str)
  df['year_month'] = pd.to_datetime(df['year_month']).apply(lambda x: x.strftime('%Y-%B'))
  df.drop(['ano','variable'],axis=1 ,inplace=True)
  df.rename(columns = {'estado':'uf', 'value':'volume'}, inplace = True)
  df = df[['year_month', 'uf', 'product', 'unit', 'volume']]
  df['volume'] = pd.to_numeric(df['volume'])
  df = df.fillna(0)
  dt = datetime.now()
  df['created_at'] = datetime.timestamp(dt)
  return df

default_args = {
    'owner': 'Raphaela Maia',
    'start_date': datetime.now()
    }

# Defining the DAG using Context Manager
with DAG(
        'extract-anp',
        default_args=default_args,
        schedule_interval=None,
        ) as dag:
        t1 = PythonOperator(
                task_id = 'task_1',
                python_callable = xlsx,
        )
        t2 = DockerOperator(
        task_id='docker_command',
        image='ipunktbs/docker-libreoffice-headless',
        auto_remove=True,
        command="['libreoffice', '--headless', '--convert-to', 'xlsx', 'vendas-combustiveis-m3.xls', '--outdir', './converted/'']",
        )
        t3 = PythonOperator(
                task_id = 'task_2',
                python_callable = create_df,
        )
        t4 = PythonOperator(
                task_id = 'task_3',
                python_callable = alter_table,
                op_kwargs={'df'},
        )




        t1 >> t2 >> t3 >> t4# Defining the task dependencies
