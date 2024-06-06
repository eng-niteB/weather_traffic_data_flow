# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from typing import List

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!!!DEV!!!
#remover na versao final
from utils.spark import get_spark_session,create_table,adjust_origin_column_name
from scripts.weather_extraction import get_weather_schema
from utils.config import timer_func,load_env_variables
load_env_variables()

@timer_func
def get_trusted_schema() -> T.StructType:
    """
    Descreve e retorna a estrutura de dados esperada para a tabela de clima na trusted zone
    
    Retorno:
    StructType: Estrutura que será utilizada para montar o DataFrame
    """
    schema = T.StructType([
        T.StructField("nu_cidade", T.IntegerType(), True),
        T.StructField("cidade", T.StringType(), True),
        T.StructField("pais", T.StringType(), True),
        T.StructField("longitude", T.FloatType(), True),
        T.StructField("latitude", T.FloatType(), True),
        T.StructField("descricao_tempo", T.StringType(), True),
        T.StructField("temperatura", T.FloatType(), True),
        T.StructField("sensacao_termica", T.FloatType(), True),
        T.StructField("temperatura_minima", T.FloatType(), True),
        T.StructField("temperatura_maxima", T.FloatType(), True),
        T.StructField("pressao", T.IntegerType(), True),
        T.StructField("umidade", T.IntegerType(), True),
        T.StructField("nivel_do_mar", T.IntegerType(), True),
        T.StructField("nivel_do_solo", T.IntegerType(), True),
        T.StructField("visibilidade", T.IntegerType(), True),
        T.StructField("velocidade_vento", T.FloatType(), True),
        T.StructField("direcao_vento", T.IntegerType(), True),
        T.StructField("rajada_vento", T.FloatType(), True),
        T.StructField("nascer_do_sol", T.LongType(), True),
        T.StructField("por_do_sol", T.LongType(), True),
        T.StructField("dt_carga", T.LongType(), True),
        T.StructField("dt", T.StringType(), True)
    ])
    
    return schema

def get_transform_raw(table_path: str, raw_schema: T.StructType, trusted_schema: T.StructType, critical_fields: List[str]) -> DataFrame:
    """
    Busca os dados da raw zone faz os tratamentos, traduz as colunas e retorna o novo DataFrame
    
    Parametros:
    table_path (str): Indica o caminho da tabela da raw zone na estrutura atual
    
    Retorno:
    Dataframe: Dataframe tratado para ser inserido na trusted
    """
    df_raw = spark.read.parquet(table_path)
    df_raw.dropna(subset=critical_fields)
    return adjust_origin_column_name(df_raw,raw_schema,trusted_schema)
    

if __name__ == "__main__":
    #Definindo variáveis especificas para a tabela dos dados climaticos
    schema : str = 'trusted'
    table_name : str = 'dados_climaticos'
    raw_table_name : str = 'weather_data'
    key_column : str = 'nu_cidade'
    order_column : str = "dt_carga"
    optional_fields : List[str] = ['sea_level','grnd_level','visibility','wind_gust']
    critical_fields : List[str] = ['id','city','country','lon','lat','weather_description','temp','feels_like','temp_min','temp_max','pressure','humidity','wind_speed','wind_deg','sunrise','sunset','load_dt','dt']
    
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = os.getenv('DATABASE_DIR')
    
    #Montando caminhos especificos da camda e da tabela
    schema_dir : str = f"{database_dir}/{schema}"
    trusted_dir : str = f"{schema_dir}/{table_name}"
    raw_dir : str = f"{database_dir}/raw/{raw_table_name}"
    
    #Criando sessão spark
    spark = get_spark_session()
    
    #Coletando a estrutura da tabela
    trusted_schema = get_trusted_schema()
    raw_schema = get_weather_schema()
    
    #Verificando se a tabela existe e se não criando-a
    create_table(spark,trusted_dir,table_name,trusted_schema)
    
    df_raw = get_transform_raw(raw_dir,raw_schema,trusted_schema,critical_fields)
    
    #Buscando os dados existentes na trusted zone
    df_trusted = spark.read.parquet(trusted_dir)
    
