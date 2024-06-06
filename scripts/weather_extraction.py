import os
import sys
import requests
from pyspark.sql import SparkSession
from typing import Dict, Any, List
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window
from datetime import datetime
import shutil
import argparse

"""
Exemplo execução do código:

spark-submit scripts/weather_extraction.py --citys "Volta Redonda" --dt "2024-06-06" > output.log 2> error.log

Busca os dados da cidade de Volta Redonda para a data do dia 2024-06-06 e insere na camada da raw zone na tabela weather_data
"""

# Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import load_env_variables
from utils.timer import timer_func

load_env_variables()

@timer_func
def get_spark_session(jar_path: str, temp_dir: str) -> SparkSession:
    """
    Retorna a sessao de spark configurada
    
    Argumentos:
    
    jar_path (str): Indica o caminho dos arquivos jars no ambiente
    temp_dir (str): Indica o diretorio para os arquivos temporarios do spark
    
    Retorno:
    
    SparkSession: Sessão do spark que será utilizada no código
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
def create_table(spark : SparkSession,table_dir: str, table_name: str, schema : T.StructType, partition_column : str) -> None:
    """
    Cria uma tabela como um arquivo parquet para simular um 'CREATE TABLE' no hadoop
    
    Argumentos:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    dir (str): diretório destino da tabela incluindo schema + nome da tabela
    table_name (str): Nome da tabela sendo criada
    schema (StructType): Estrutura da tabela destino incluindo colunas e tipos
    partition_column (str): coluna de particionamento da tabela
    """
    #Criando dados da carga inicial
    if not check_if_table_exists(table_dir):
        num_columns = len(schema.fields)
        initial_data = [(None,) * num_columns]
        
        empty_df = spark.createDataFrame(initial_data, schema)    
        empty_df.write.partitionBy(partition_column).parquet(table_dir)
    
    else:
        print(f'Tabela {table_name} já existe no ambiente')

@timer_func   
def format_weather_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formata o Json retornado pela API para a estrutura esperada no dataFrame, além de criar a coluna dt
    
    Argumentos:
    
    data (Dict[str, Any]): Json retornado pela API
    
    Retorno:
    Dict[str, Any]: Json formatado já seguindo a estrutura padrão da tabela e com a nova coluna criada
    """
    dtime = datetime.fromtimestamp(data['dt'])
    dt = dtime.strftime('%Y-%m-%d')
        
    formatted_data = {
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

    return formatted_data

@timer_func
def check_if_table_exists(table_dir: str) -> bool:
    """
    Verifica se o diretório da tabela existe e testa algumas condições:
    1 - Se existir e estiver vazio, exclui o diretório e retorna False.
    2 - Se existir e não estiver vazio, retorna True.
    3 - Se não existir, retorna False.

    Argumentos:
    table_dir (str): Caminho do diretório da tabela.

    Retorno:
    bool: True se o diretório existir e não estiver vazio, False caso contrário.
    """
    try:
        if os.path.isdir(table_dir):
            if len(os.listdir(table_dir)) == 0:
                shutil.rmtree(table_dir)
                return False
            return True
        return False
    except Exception as err:
        print(f"Erro ao verificar o diretório {table_dir}: {err}")
        return False     

@timer_func
def remove_duplicates(df: DataFrame, key_column: str, order_column : str) -> DataFrame:
    """
    Remove linhas duplicadas do dataframe
    
    
    Parametros:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    df (DataFrame): DataFrame de onde serão retiradas as duplicidades
    key_column (str): Coluna chave primaria da tabela
    order_column (str): Coluna de ordenação da tabela    
    
    Retorno:
    DataFrame : DataFrame sem duplicadas
    """
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc())
    
    df = df.withColumn('row_num', F.row_number().over(window))
    df = df.filter(F.col('row_num') == 1)
    df = df.drop('row_num')
    
    return df

@timer_func
def insert_data(spark : SparkSession,table_dir: str, table_name: str, schema : T.StructType, key_column: str, partition_column : str, order_column : str, dt : str) -> None:
    """
    Insere os dados novos na tabela destino, tratando eles antes da insercao
    
    Argumentos:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    dir (str): diretório destino da tabela incluindo schema + nome da tabela
    table_name (str): Nome da tabela sendo criada
    schema (StructType): Estrutura da tabela destino incluindo colunas e tipos
    key_column (str): Coluna chave primaria da tabela
    partition_column (str): coluna de particionamento da tabela
    order_column (str): coluna de ordenacao da tabela
    dt (str): data referencia da carga
    """
    response = get_weather_data(citys)
    weather_data = response[0]
    errors = response[1]
    new_datas = []
        
    df_insert = spark.createDataFrame([],schema)
    df_table = spark.read.parquet(table_dir)
    df_table = df_table.filter(F.col('id').isNotNull())
    df_destiny = df_table.filter(F.col('dt') == dt)

    
    if errors:
        for error in errors:
            raise ValueError(f"status: {error.get('status', 500)}  \n Error: {error['error']}")
    else:
        for data in weather_data:
            new_data = format_weather_data(data)
            new_datas.append(new_data)
            

        df_insert = spark.createDataFrame(new_datas,schema)
        df_destiny = df_destiny.unionByName(df_insert)
        df_destiny = remove_duplicates(df_destiny, key_column, order_column)
        df_destiny.write.mode("overwrite").partitionBy(partition_column).parquet(table_dir)
        print(f'Dado inserido na tabela {table_name} para a partição {dt}')

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
    insert_data(spark,table_dir,table_name,weather_schema,key_column,partition_column,order_column,dt)
    df = spark.read.parquet(table_dir)
    print(df.show())
    spark.stop()