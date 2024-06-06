import os
import sys
import requests
from pyspark.sql import SparkSession
from typing import Dict, Any, List
from pyspark.sql import types as T
from pyspark.sql import DataFrame

# Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import load_env_variables
from utils.timer import timer_func

load_env_variables()

@timer_func
def get_spark_session(jar_path: str, temp_dir: str) -> SparkSession:
    """
    Retorna a sessao de spark configurada
    """
    spark = SparkSession.builder \
        .appName("ETLJob") \
        .config("spark.jars", jar_path) \
        .config("spark.local.dir", temp_dir) \
        .config("spark.sql.warehouse.dir", "/tmp/warehouse") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.nativeio.impl.disable.cache", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

@timer_func
def get_weather_data(citys: List[str]) -> List[Dict[str, Any]]:
    """
    Busca os dados na API https://openweathermap.org/api a partir da lista de cidades passada
    
    Parametros:
    citys Dict[str]: Dicionario com todas as cidades para que devem ser feitas as requisicoes
    
    Retorno:
    Dict: As informações de clima em caso de sucesso e um array vazio em caso de falha.
    """
    weather_key = os.getenv('WEATHER_KEY')
    
    weather = []
    errors = []
    
    for city in citys:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={weather_key}&lang=pt-br"
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
    """
    schema = T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("city", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("lon", T.FloatType(), True),
            T.StructField("lat", T.FloatType(), True),
            T.StructField("weather_description", T.StringType(), True),
            T.StructField("temp_f", T.FloatType(), True),
            T.StructField("feels_like_f", T.FloatType(), True),
            T.StructField("temp_min_f", T.FloatType(), True),
            T.StructField("temp_max_f", T.FloatType(), True),
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
def create_table(spark : SparkSession,dir: str, table_name: str, schema : T.StructType, partitionKey : str) -> None:
    #Criando dados da carga inicial
    num_columns = len(schema.fields)
    initial_data = [(None,) * num_columns]
    
    empty_df = spark.createDataFrame(initial_data, schema)    
    empty_df.write.partitionBy(partitionKey).parquet(dir)

if __name__ == "__main__":    
    citys = ['Divinopolis']
    schema = 'raw'
    table_name = 'weather_data'
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    temp_dir = os.getenv('TEMP_DIR')
    jar_path = os.getenv('POSTGRES_JAR_PATH')
    database_dir = os.getenv('DATABASE_DIR')
    
    schema_dir = f"{database_dir}/{schema}"
    table_dir = f"{schema_dir}/{table_name}"
    partitionKey = "dt"
    spark = get_spark_session(jar_path,temp_dir)
    
    weather_schema = get_weather_schema()
    
    #create_table(spark,table_dir,table_name,weather_schema,partitionKey)
    
    df = spark.read.parquet(table_dir)
    print(df.show())
    spark.stop()
    
    

