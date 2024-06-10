# -*- coding: utf-8 -*-
import os
import sys
from typing import List

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark import get_spark_session,create_table,get_transform_raw,insert_trusted_data
from utils.config import read_secret,get_args
from utils.timer import timer_func
from models.weather import Weather
from models.traffic import Traffic

if __name__ == "__main__":
    args = get_args()
    method = args.method
    
    if method == 'weather':
        model = Weather()
    elif method == 'traffic':
        model = Traffic()
    else:
        raise Exception(f'Método {method} não é reconhecido como uma funcionalidade desenvolvida.')
    
    #Incializando váriaveis
    schema_raw : str = model.raw_schema
    schema_trusted : str = model.trusted_schema
    table_name : str = model.trusted_name
    raw_table_name : str = model.raw_name
    partition_column : str = model.partition_column
    key_column : str = model.trusted_key_column
    order_column : str = model.trusted_order_column
    optional_fiels : List[str] = model.optional_fields
    critical_fields : List[str] = model.critical_fields
    
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = read_secret('/run/secrets/database_dir')
    
    #Montando caminhos especificos da camda e da tabela
    schema_dir : str = f"{database_dir}/{schema_trusted}"
    trusted_dir : str = f"{schema_dir}/{table_name}"
    raw_dir : str = f"{database_dir}/{schema_raw}/{raw_table_name}"
    
    #Criando sessão spark
    spark = get_spark_session()
    
    #Coletando a estrutura da tabela
    trusted_schema = model.get_trusted_schema()
    raw_schema = model.get_raw_schema()
    
    #Verificando se a tabela existe e se não criando-a
    create_table(spark,trusted_dir,table_name,trusted_schema)
    
    #Traduzindo as colunas da raw zone para a trusted zone
    df_raw = get_transform_raw(raw_dir,raw_schema,trusted_schema,critical_fields)
    
    #Buscando os dados existentes na trusted zone
    df_trusted = spark.read.parquet(trusted_dir)
    
    #Inserindo novos dados na trusted zone
    insert_trusted_data(df_raw,df_trusted, key_column, order_column,trusted_dir)
    
    spark.stop()   
    