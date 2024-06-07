# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import types as T
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass
import requests

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import timer_func, load_env_variables
load_env_variables()

@dataclass(frozen=True)
class Weather:
    raw_schema: str = 'raw'
    raw_name: str = 'weather_data'
    raw_key_column: str = 'id'
    raw_order_column: str = "load_dt"
    trusted_schema: str = 'trusted'
    trusted_name: str = 'dados_climaticos'
    trusted_key_column : str = 'nu_cidade'
    trusted_order_column : str = "dt_carga"
    partition_column: str = "dt"

    
    @staticmethod
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

    @staticmethod
    def get_raw_schema() -> T.StructType:
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

    @staticmethod
    @timer_func
    def get_api_data(citys: List[str]) -> List[Dict[str, Any]]:
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

    @staticmethod
    @timer_func   
    def format_data(citys: str,dt: str) -> List[Dict[str, Any]]:
        """
        Formata o Json retornado pela API para a estrutura esperada no dataFrame, além de criar a coluna dt
        
        Argumentos:
            data (Dict[str, Any]): Json retornado pela API
            dt (str): Data de referência para a extração dos dados
        
        Retorno:
        Dict[str, Any]: Json formatado já seguindo a estrutura padrão da tabela e com a nova coluna criada
        """
        response = Weather.get_api_data(citys)
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

