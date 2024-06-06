# -*- coding: utf-8 -*-
import os
import sys
import requests
from typing import Dict, Any, List

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
def get_traffic_data(routes: List[Dict[str, str]]) -> List[Dict[str, Any]]:
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
        origin: str = route['origin']
        destination: str = route['destination']
        mode: str = "driving"
        url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}&destination={destination}&mode={mode}&key={api_key}&language=pt-BR"
        try:
            response = requests.get(url)
            response.raise_for_status()
            traffic_data.append(response.json())
        except requests.exceptions.HTTPError as http_err:
            errors.append({'status': response.status_code, 'error': str(http_err)})
        except requests.exceptions.RequestException as req_err:
            errors.append({'status': 'Request Error', 'error': str(req_err)})
        except Exception as err:
            errors.append({'status': 'Error', 'error': str(err)})    
            
    return [traffic_data, errors]
              
if __name__ == "__main__":    
    # Definindo variáveis específicas para a tabela dos dados climáticos
    schema: str = 'raw'
    table_name: str = 'traffic_data'
    partition_column: str = "dt"
    key_column: str = 'id'
    order_column: str = "load_dt"
    
    routes = [{"origin": "Divinópolis", "destination": "Contagem"}]
    
    data = get_traffic_data(routes)
    print(data)
