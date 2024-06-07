# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import types as T
from pyspark.sql import functions as F
from typing import List

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import timer_func,load_env_variables
from utils.spark import get_spark_session,create_table,get_transform_raw,insert_trusted_data
from scripts.traffic_extraction import get_traffic_schema

load_env_variables()

@timer_func
def get_trusted_schema() -> T.StructType:
    """
    Descreve e retorna a estrutura de dados esperada para a tabela de trafego na trusted zone
    
    Retorno:
    StructType: Estrutura que será utilizada para montar o DataFrame
    """
    schema = T.StructType([
            T.StructField("nu_rota", T.StringType(), True),
            T.StructField("nu_cidade_origem", T.IntegerType(), True),
            T.StructField("cidade_origem", T.StringType(), True),
            T.StructField("nu_cidade_destino", T.IntegerType(), True),
            T.StructField("cidade_destino", T.StringType(), True),
            T.StructField("localizacao_inicial", T.StringType(), True),
            T.StructField("localizacao_final", T.StringType(), True),
            T.StructField("cd_distancia", T.StringType(), True),
            T.StructField("distancia", T.IntegerType(), True),
            T.StructField("cd_duracao", T.StringType(), True),
            T.StructField("duracao", T.IntegerType(), True),
            T.StructField("passos", T.ArrayType(T.MapType(T.StringType(), T.StringType())), True),
            T.StructField("dt_carga", T.LongType(), True),
            T.StructField("dt", T.StringType(), True)
        ])
    
    return schema

if __name__ == "__main__":
    #Definindo variáveis especificas para a tabela dos dados das Rotas
    schema : str = 'trusted'
    table_name : str = 'dados_trafego'
    raw_table_name : str = 'traffic_data'
    key_column : str = 'nu_rota'
    order_column : str = "dt_carga"
    optional_fields : List[str] = []
    critical_fields : List[str] = ["route_uuid","origin_uuid","origin_city","destination_uuid","destination_city","destination_start","destination_end","cd_travel_distance","travel_distance","cd_travel_duration","travel_duration","steps","load_dt","dt"]
    
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = os.getenv('DATABASE_DIR')
    
    #Montando caminhos especificos da camda e da tabela
    schema_dir : str = f"{database_dir}/{schema}"
    trusted_dir : str = f"{schema_dir}/{table_name}"
    raw_dir : str = f"{database_dir}/raw/{raw_table_name}"
    
    #Criando sessão spark
    spark = get_spark_session()
    
    #Coletando a estrutura da tabela
    raw_schema = get_traffic_schema()
    trusted_schema = get_trusted_schema()
    
    #Verificando se a tabela existe e se não criando-a
    create_table(spark,trusted_dir,table_name,trusted_schema)
    
    df_raw = get_transform_raw(raw_dir,raw_schema,trusted_schema,critical_fields)
    
    #Buscando os dados existentes na trusted zone
    df_trusted = spark.read.parquet(trusted_dir)
    
    #Inserindo novos dados na trusted zone
    insert_trusted_data(df_raw,df_trusted, key_column, order_column,trusted_dir)
    
    spark.stop()