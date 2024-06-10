# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import types as T
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass, field
import requests

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import timer_func,read_secret

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
    optional_fields: List[str] = field(default_factory=lambda: ['sea_level','grnd_level','visibility','wind_gust'])
    critical_fields: List[str] = field(default_factory=lambda: ['id','city','country','lon','lat','weather_description','temp','feels_like','temp_min','temp_max','pressure','humidity','wind_speed','wind_deg','sunrise','sunset','load_dt','dt'])
        
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
        weather_key = read_secret('/run/secrets/weather_key')
        
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
        
        
        for data in weather_data:
            dtime = datetime.fromtimestamp(data['dt'])
            dt = dtime.strftime('%Y-%m-%d')
            new_data = {
                'id': data.get('id'),
                'city': data.get('name', None),
                'country': data.get('sys', {}).get('country', None),
                'lon': float(data.get('coord', {}).get('lon', 0.0)),
                'lat': float(data.get('coord', {}).get('lat', 0.0)),
                'weather_description': data.get('weather', [{}])[0].get('description', None),
                'temp': float(data.get('main', {}).get('temp', 0.0)),
                'feels_like': float(data.get('main', {}).get('feels_like', 0.0)),
                'temp_min': float(data.get('main', {}).get('temp_min', 0.0)),
                'temp_max': float(data.get('main', {}).get('temp_max', 0.0)),
                'pressure': int(data.get('main', {}).get('pressure', 0)),
                'humidity': int(data.get('main', {}).get('humidity', 0)),
                'sea_level': int(data.get('main', {}).get('sea_level', 0)) if 'sea_level' in data.get('main', {}) else None,
                'grnd_level': int(data.get('main', {}).get('grnd_level', 0)) if 'grnd_level' in data.get('main', {}) else None,
                'visibility': int(data.get('visibility', 0)),
                'wind_speed': float(data.get('wind', {}).get('speed', 0.0)),
                'wind_deg': int(data.get('wind', {}).get('deg', 0)),
                'wind_gust': float(data.get('wind', {}).get('gust', 0.0)) if 'gust' in data.get('wind', {}) else None,
                'sunrise': int(data.get('sys', {}).get('sunrise', 0)),
                'sunset': int(data.get('sys', {}).get('sunset', 0)),
                'load_dt': int(data.get('dt', 0)),
                'dt': datetime.fromtimestamp(data.get('dt', 0)).strftime('%Y-%m-%d')
            }
            new_datas.append(new_data)
            
        if errors:
            for error in errors:
                 print(ValueError(f"status: {error.get('status', 500)}  \n Error: {error['error']}"))

        return new_datas

