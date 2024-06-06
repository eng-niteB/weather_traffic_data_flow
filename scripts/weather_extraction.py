import os
import sys
import requests
from pyspark.sql import SparkSession
from typing import Dict, Any, List

# Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import load_env_variables
from utils.timer import timer_func

load_env_variables()

@timer_func
def get_spark_session(temp_dir: str) -> SparkSession:
    """
    Retorna a sessao de spark configurada
    """
    spark = SparkSession.builder \
        .appName("ETLJob") \
        .config("spark.jars", jar_path) \
        .config("spark.local.dir", temp_dir) \
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

if __name__ == "__main__":
    citys = ['Divinopolis']
    schema = 'raw'
    table_name = 'weather_data'
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    temp_dir = os.getenv('TEMP_DIR')
    jar_path = os.getenv('POSTGRES_JAR_PATH')
        
    api_data = get_weather_data(citys)
    weather = api_data[0]
    errors = api_data[1]
    
    print(f"""
          weather_data : {weather}
          errors: {errors}
          """)

