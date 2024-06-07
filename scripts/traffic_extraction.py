# -*- coding: utf-8 -*-
import os
import sys
import requests
import uuid
from typing import Dict, Any, List
from pyspark.sql import types as T
from datetime import datetime

"""
OBJETIVO: Coletar os dados da API do Maps e disponibilizá-los na camada da raw no Hadoop
"""

# Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!!!DEV!!!
# Remover na versão final
from utils.config import load_env_variables
load_env_variables()

from utils.timer import timer_func
from utils.spark import get_spark_session, create_table, insert_data

@timer_func
def get_traffic_data(routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Busca as informações das rotas entre as cidades onde a Empresa atua e retorna o DataFrame com os dados
    
    Parâmetros:
    routes (List[Dict[str, str]]): Lista com todas as rotas possíveis entre as cidades
    
    Retorno:
    List[Dict[str, Any]]: Lista com as informações das rotas passadas
    """
    api_key: str = os.getenv('MAPS_KEY')
    
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

@timer_func
def format_traffic_data(routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Formata os Jsons retornados pela API para a estrutura esperada do DataFrame
    
    Argumentos:
    routes (List[Dict[str, Any]]): Lista com todas as rotas possíveis para serem tratadas
    
    Retorno:
    List[Dict[str, Any]]: Json formatado já seguindo a estrutura padrão da tabela e com a nova coluna criada
    """
    response = get_traffic_data(routes)
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
                ts = datetime.timestamp(now)
                dt = now.strftime('%Y-%m-%d')
                new_data = {
                    'route_uuid': str(uuid.uuid4()),
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

@timer_func
def get_traffic_schema() -> T.StructType:
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

if __name__ == "__main__":    
    # Definindo variáveis específicas para a tabela dos dados climáticos
    schema: str = 'raw'
    table_name: str = 'traffic_data'
    partition_column: str = "dt"
    key_column: str = 'id'
    order_column: str = "load_dt"
    
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = os.getenv('DATABASE_DIR')
    
    #Montando caminhos especificos da camda e da tabela
    schema_dir : str = f"{database_dir}/{schema}"
    table_dir : str = f"{schema_dir}/{table_name}"
    
    #Coletando a estrutura da tabela
    schema = get_traffic_schema()
    
    routes = [{
        "origin": {
            "nu_cidade": 1234,
            "cidade": "Divinópolis"
        },
        "destination": {
            "nu_cidade": 2345,
            "cidade": "Contagem"
        }
    }]
    
    spark = get_spark_session()
    
    #Verificando se a tabela existe e se não criando-a
    create_table(spark,table_dir,table_name,schema,partition_column)
    
    data = format_traffic_data(routes)
    print(data)
