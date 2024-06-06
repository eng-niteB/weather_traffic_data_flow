# -*- coding: utf-8 -*-
import os
import sys
import requests
from pyspark.sql import SparkSession
from typing import Dict, Any, List
from pyspark.sql import types as T
from pyspark.sql import functions as F
from datetime import datetime
import argparse

"""
Exemplo execução do código:

spark-submit scripts/weather_extraction.py --citys "Volta Redonda" --dt "2024-06-06" > output.log 2> error.log

Busca os dados da cidade de Volta Redonda para a data do dia 2024-06-06 e insere na camada da raw zone na tabela weather_data
"""

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import load_env_variables
from utils.timer import timer_func
from utils.spark import get_spark_session,create_table,insert_data

load_env_variables()

@timer_func
def get_weather_data(citys: List[str]) -> List[Dict[str, Any]]:
    """
    Busca os dados na API https://openweathermap.org/api a partir da lista de cidades passada
    
    Parametros:
    citys Dict[str]: Dicionario com todas as cidades para que devem ser feitas as requisicoes
    dt (str): Data referencia para a busca dos dados
    
    Retorno:
    Dict: As informações de clima em caso de sucesso e um array vazio em caso de falha.
    """
    weather_key = os.getenv('WEATHER_KEY')
    
    weather = []
    errors = []
    
    for city in citys:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={weather_key}&units=metric&lang=pt_br"
        try:
            response = requests.get(url)
            response.raise_for_status()
            weather.append(response.json())
        except requests.exceptions.HTTPError as http_err:
            errors.append({'status': response.status_code, 'error': str(http_err)})
        except requests.exceptions.RequestException as req_err:
            errors.append({'status': 'Request Error', 'error': str(req_err)})
        except Exception as err:
            errors.append({'status': 'Error', 'error': str(err)})
            
    return [weather, errors]

@timer_func
def get_weather_schema() -> T.StructType:
    """
    Descreve e retorna a estrutura de dados esperada para a tabela de clima
    
    Retorno:
    StructType: Estrutura que será utilizada para montar o DataFrame
    """
    schema = T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("city", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("lon", T.FloatType(), True),
            T.StructField("lat", T.FloatType(), True),
            T.StructField("weather_description", T.StringType(), True),
            T.StructField("temp", T.FloatType(), True),
            T.StructField("feels_like", T.FloatType(), True),
            T.StructField("temp_min", T.FloatType(), True),
            T.StructField("temp_max", T.FloatType(), True),
            T.StructField("pressure", T.IntegerType(), True),
            T.StructField("humidity", T.IntegerType(), True),
            T.StructField("sea_level", T.IntegerType(), True),
            T.StructField("grnd_level", T.IntegerType(), True),
            T.StructField("visibility", T.IntegerType(), True),
            T.StructField("wind_speed", T.FloatType(), True),
            T.StructField("wind_deg", T.IntegerType(), True),
            T.StructField("wind_gust", T.FloatType(), True),
            T.StructField("sunrise", T.LongType(), True),
            T.StructField("sunset", T.LongType(), True),
            T.StructField("load_dt", T.LongType(), True),
            T.StructField("dt", T.StringType(), True)
        ])
    
    return schema

@timer_func   
def format_weather_data(citys: str) -> Dict[str, Any]:
    """
    Formata o Json retornado pela API para a estrutura esperada no dataFrame, além de criar a coluna dt
    
    Argumentos:
    
    data (Dict[str, Any]): Json retornado pela API
    
    Retorno:
    Dict[str, Any]: Json formatado já seguindo a estrutura padrão da tabela e com a nova coluna criada
    """
    response = get_weather_data(citys)
    weather_data = response[0]
    errors = response[1]
    new_datas = []
    
    if errors:
        for error in errors:
            raise ValueError(f"status: {error.get('status', 500)}  \n Error: {error['error']}")
    else:
        for data in weather_data:
            dtime = datetime.fromtimestamp(data['dt'])
            dt = dtime.strftime('%Y-%m-%d')
            new_data = {
                'id': data['id'],
                'city': data['name'],
                'country': data['sys']['country'],
                'lon': data['coord']['lon'],
                'lat': data['coord']['lat'],
                'weather_description': data['weather'][0]['description'],
                'temp': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'pressure': data['main']['pressure'],
                'humidity': data['main']['humidity'],
                'sea_level': data['main'].get('sea_level'),  # Campo opcional
                'grnd_level': data['main'].get('grnd_level'),  # Campo opcional
                'visibility': data.get('visibility'),  # Campo opcional
                'wind_speed': data['wind']['speed'],
                'wind_deg': data['wind']['deg'],
                'wind_gust': data['wind'].get('gust'),  # Campo opcional
                'sunrise': data['sys']['sunrise'],
                'sunset': data['sys']['sunset'],
                'load_dt': data['dt'],
                'dt' : dt
            }
            new_datas.append(new_data)

    return new_datas

@timer_func
def get_args() -> argparse.Namespace:
    """
    Define os parametros passados durante a execução do código e retorna seus valores
    
    Retorno:
    argparse.Namespace: Objeto com os valores dos argumentos
    """
    parser = argparse.ArgumentParser(description="Argumentos para a carga da tabela")
    parser.add_argument('--citys', type=str, nargs='+', required=True, help="Relacao de cidades para serem carregadas")
    parser.add_argument('--dt', type=str, required=True, help="Indica a data referencia da execucao")
    return parser.parse_args()
            
if __name__ == "__main__":
    args = get_args()
    citys = args.citys
    dt = args.dt
    
    schema = 'raw'
    table_name = 'weather_data'
    partition_column = "dt"
    key_column = 'id'
    order_column = "load_dt"
    
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    temp_dir = os.getenv('TEMP_DIR')
    jar_path = os.getenv('POSTGRES_JAR_PATH')
    database_dir = os.getenv('DATABASE_DIR')
    
    schema_dir = f"{database_dir}/{schema}"
    table_dir = f"{schema_dir}/{table_name}"

    spark = get_spark_session(jar_path,temp_dir)
    
    weather_schema = get_weather_schema()
    
    create_table(spark,table_dir,table_name,weather_schema,partition_column)
    
    df = spark.read.parquet(table_dir)
    print(df.show())
    
    new_data = format_weather_data(citys)
    
    insert_data(spark,table_dir,table_name,weather_schema,key_column,partition_column,order_column,dt,new_data)
    
    df = spark.read.parquet(table_dir)
    
    print(df.show())
    spark.stop()