# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import types as T
from pyspark.sql import functions as F
from typing import List
import argparse

"""
Objetivo:
    Buscar os dados necessários das APIS e inseri-los na camada da RAW
    
Bash uso:
    Weather:
        spark-submit scripts/insert_raw_data.py --method "weather" --citys "Resende" --dt "2024-06-07" > out.log 2> err.log
        
    Traffic:
        spark-submit scripts/insert_raw_data.py --method "traffic" --origin "Resende" --destination "Lavras" --dt "2024-06-07" > out.log 2> err.log
"""

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!!!DEV!!!
#remover na versao final
from utils.spark import get_spark_session,create_table,insert_data,get_citys_data
from utils.config import timer_func,load_env_variables
from models.weather import Weather
from models.traffic import Traffic
load_env_variables()

@timer_func
def get_args() -> argparse.Namespace:
    """
    Define os parametros passados durante a execução do código e retorna seus valores
    
    Retorno:
    argparse.Namespace: Objeto com os valores dos argumentos
    """
    parser = argparse.ArgumentParser(description="Argumentos para a carga da tabela")
    parser.add_argument('--method', type=str, required=True, help="Indica se será uma carga de 'weather' ou 'traffic'")
    parser.add_argument('--citys', type=str, nargs='+', required=False, help="Relacao de cidades para serem carregadas")
    parser.add_argument('--origin', type=str, required=False, help="Cidade de origem")
    parser.add_argument('--destination', type=str, required=False, help="Cidade de destino")
    parser.add_argument('--dt', type=str, required=True, help="Indica a data referencia da execucao")
    return parser.parse_args()

if __name__ == "__main__":
    #Coletando argumentos passados via linha de comando
    args = get_args()
    method = args.method
    dt = args.dt
        
    if method == 'weather':
        model = Weather()
        citys : List[str] = args.citys
    elif method == 'traffic':
        model = Traffic()
        origin : str = args.origin
        destination : str = args.destination
    else:
        raise Exception(f'Método {method} não é reconhecido como uma funcionalidade desenvolvida.')
    
    #Incializando váriaveis
    schema : str = model.raw_schema
    table_name : str = model.raw_name
    partition_column : str = model.partition_column
    key_column : str = model.raw_key_column
    order_column : str = model.raw_order_column
    
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = os.getenv('DATABASE_DIR')
    
    #Montando caminhos especificos da camda e da tabela
    schema_dir : str = f"{database_dir}/{schema}"
    table_dir : str = f"{schema_dir}/{table_name}"

    #Criando sessão spark
    spark = get_spark_session()
    
    if method == 'traffic':
        model = Traffic()
        try:
            citys_dir = f"{database_dir}/{model.trusted_schema}/{model.citys_table}"
            citys_data = get_citys_data(spark,citys_dir,origin,destination,dt)
            citys = [{
                "origin": {
                    "nu_cidade": citys_data[0]['nu_cidade'],
                    "cidade": citys_data[0]['cidade']
                },
                "destination": {
                    "nu_cidade": citys_data[1]['nu_cidade'],
                    "cidade": citys_data[1]['cidade']
                }
            }]
        except ValueError as e:
            raise Exception(str(e))
    
    #Coletando a estrutura da tabela
    table_schema = model.get_raw_schema()
    
    #Verificando se a tabela existe e se não criando-a
    create_table(spark,table_dir,table_name,table_schema,partition_column)
    
    #Coletando os novos dados 
    new_data = model.format_data(citys,dt)
    
    #Inserindo os novos dados na tabela
    insert_data(spark,table_dir,table_name,table_schema,key_column,order_column,dt,new_data,partition_column)
    
    #Encerrando a sessão do Spark
    spark.stop()