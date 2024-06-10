# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import types as T
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass,field
import requests

# Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import timer_func,read_secret

@dataclass(frozen=True)
class Traffic:
    raw_schema: str = 'raw'
    raw_name: str = 'traffic_data'
    raw_key_column: str = 'route_uuid'
    raw_order_column: str = "load_dt"
    trusted_schema: str = 'trusted'
    trusted_name: str = 'dados_trafego'
    trusted_key_column: str = 'nu_rota'
    trusted_order_column: str = "dt_carga"
    partition_column: str = "dt"
    citys_table : str = 'dados_climaticos'
    optional_fields : List[str] = field(default_factory=lambda : [])
    critical_fields : List[str] = field(default_factory=lambda : ["route_uuid","origin_uuid","origin_city","destination_uuid","destination_city","destination_start","destination_end","cd_travel_distance","travel_distance","cd_travel_duration","travel_duration","steps","load_dt","dt"])
        
    @staticmethod
    def get_raw_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego
        
        Retorno:
        StructType: Estrutura que será utilizada para montar o DataFrame
        """
        schema = T.StructType([
                T.StructField("route_uuid", T.StringType(), True),
                T.StructField("origin_uuid", T.IntegerType(), True),
                T.StructField("origin_city", T.StringType(), True),
                T.StructField("destination_uuid", T.IntegerType(), True),
                T.StructField("destination_city", T.StringType(), True),
                T.StructField("destination_start", T.StringType(), True),
                T.StructField("destination_end", T.StringType(), True),
                T.StructField("cd_travel_distance", T.StringType(), True),
                T.StructField("travel_distance", T.IntegerType(), True),
                T.StructField("cd_travel_duration", T.StringType(), True),
                T.StructField("travel_duration", T.IntegerType(), True),
                T.StructField("steps", T.ArrayType(T.MapType(T.StringType(), T.StringType())), True),
                T.StructField("load_dt", T.LongType(), True),
                T.StructField("dt", T.StringType(), True)
            ])
        
        return schema
    
    @staticmethod
    def get_trusted_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego na trusted zone
        
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
    
    @staticmethod
    @timer_func
    def get_api_data(routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Busca as informações das rotas entre as cidades onde a Empresa atua e retorna o DataFrame com os dados
        
        Parâmetros:
        routes (List[Dict[str, str]]): Lista com todas as rotas possíveis entre as cidades
        
        Retorno:
        List[Dict[str, Any]]: Lista com as informações das rotas passadas
        """
        api_key: str = read_secret('/run/secrets/maps_key')
        
        traffic_data: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        
        for route in routes:
            origin: str = route['origin']['cidade']
            destination: str = route['destination']['cidade']
            mode: str = "driving"
            url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}&destination={destination}&mode={mode}&key={api_key}&language=pt-BR"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                data['route'] = route
                traffic_data.append(data)
            except requests.exceptions.HTTPError as http_err:
                errors.append({'status': response.status_code, 'error': str(http_err)})
            except requests.exceptions.RequestException as req_err:
                errors.append({'status': 'Request Error', 'error': str(req_err)})
            except Exception as err:
                errors.append({'status': 'Error', 'error': str(err)})    
                
        return [traffic_data, errors]
    
    @staticmethod
    @timer_func
    def format_data(routes: List[Dict[str, Any]],dt : str) -> List[Dict[str, Any]]:
        """
        Formata os Jsons retornados pela API para a estrutura esperada do DataFrame
        
        Argumentos:
        routes (List[Dict[str, Any]]): Lista com todas as rotas possíveis para serem tratadas
        
        Retorno:
        List[Dict[str, Any]]: Json formatado já seguindo a estrutura padrão da tabela e com a nova coluna criada
        """
        response = Traffic.get_api_data(routes)
        datas = response[0]
        errors = response[1]
        formatted_data = []
        
        if errors:
            for error in errors:
                raise ValueError(f"status: {error.get('status', 500)}  \n Error: {error['error']}")
        else:
            for data in datas:
                for route_data in data.get("routes", []):
                    now = datetime.now()
                    ts = int(datetime.timestamp(now))
                    dt = now.strftime('%Y-%m-%d')
                    origin_uuid = data['route']['origin']['nu_cidade']
                    destination_uuid = data['route']['destination']['nu_cidade']
                    route_uuid = f"{origin_uuid}_{destination_uuid}" 
                    
                    new_data = {
                        'route_uuid': route_uuid,
                        'origin_uuid': data['route']['origin']['nu_cidade'],
                        'origin_city': data['route']['origin']['cidade'],
                        'destination_uuid': data['route']['destination']['nu_cidade'],
                        'destination_city': data['route']['destination']['cidade'],
                        'destination_start': route_data['legs'][0]['start_address'],
                        'destination_end': route_data['legs'][0]['end_address'],
                        'cd_travel_distance': route_data['legs'][0]['distance']['text'],
                        'travel_distance': route_data['legs'][0]['distance']['value'],
                        'cd_travel_duration': route_data['legs'][0]['duration']['text'],
                        'travel_duration': route_data['legs'][0]['duration']['value'],
                        'steps': route_data['legs'][0]['steps'],
                        'load_dt': ts,
                        'dt': dt
                    }
                    
                    formatted_data.append(new_data)
                
        return formatted_data
