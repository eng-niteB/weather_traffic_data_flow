import os
import sys
from pyspark.sql import types as T
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass
from pyspark.sql import SparkSession

# Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.timer import timer_func
from utils.spark import get_citys_data

@dataclass(frozen=True)
class routeDescription:
    trusted_schema: str = 'trusted'
    trusted_name: str = 'descricao_rota'
    trusted_key_column: str = 'nu_rota'
    trusted_order_column: str = "dt_carga"
    
    @staticmethod
    def get_trusted_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego na trusted zone
        
        Retorno:
        StructType: Estrutura que será utilizada para montar o DataFrame
        """
        schema = T.StructType([
                T.StructField("nu_rota", T.StringType(), True),
                T.StructField("nu_origem", T.IntegerType(), True),
                T.StructField("origem", T.StringType(), True),
                T.StructField("ponto_partida", T.StringType(), True),
                T.StructField("descricao_tempo_origem", T.StringType(), True),
                T.StructField("temp_origem", T.FloatType(), True),
                T.StructField("sensacao_termica_origem", T.FloatType(), True),
                T.StructField("temp_minima_origem", T.FloatType(), True),
                T.StructField("temp_max_origem", T.FloatType(), True),
                T.StructField("altitude_origem", T.IntegerType(), True),
                T.StructField("nu_destino", T.IntegerType(), True),
                T.StructField("destino", T.StringType(), True),  # Corrigido de "origem" para "destino"
                T.StructField("ponto_chegada", T.StringType(), True),
                T.StructField("descricao_tempo_destino", T.StringType(), True),
                T.StructField("temp_destino", T.FloatType(), True),
                T.StructField("sensacao_termica_destino", T.FloatType(), True),
                T.StructField("temp_minima_destino", T.FloatType(), True),
                T.StructField("temp_max_destino", T.FloatType(), True),
                T.StructField("altitude_destino", T.IntegerType(), True),
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
    def format_data(spark : SparkSession, weather_table_dir : str, datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Formata os Jsons retornados pela API para a estrutura esperada do DataFrame
        
        Argumentos:
        spark (SparkSession): Sessão do spark utilizada nos módulos
        weather_table_dir (str): Caminho da tabela com os dados de cidades
        datas (List[Dict[str, Any]]): Rotas existentes na tabela dados_trafego
        
        Retorno:
        List[Dict[str, Any]]: Json formatado já seguindo a estrutura padrão da tabela e com a nova coluna criada
        """
        new_data = []
        
        for data in datas:
            origin = data['cidade_origem']
            destination = data['cidade_destino']
            ts = int(datetime.timestamp(datetime.now()))
            dt = data['dt']
            
            citys_data = get_citys_data(spark,weather_table_dir,origin,destination,dt)
            
            origin_data = citys_data[0]
            destination_data = citys_data[1]
            
            print(data)
                       
            formatted_data = {
                "nu_rota" : data['nu_rota'],
                "nu_origem" : data["nu_cidade_origem"],
                "origem" : data["cidade_origem"],
                "ponto_partida" : data['localizacao_inicial'],
                "descricao_tempo_origem" : origin_data['descricao_tempo'],
                "temp_origem" : origin_data['temperatura'],
                "sensacao_termica_origem" : origin_data['sensacao_termica'],
                "temp_minima_origem" : origin_data['temperatura_minima'],
                "temp_max_origem" : origin_data['temperatura_maxima'],
                "altitude_origem" : origin_data.get('nivel_do_mar'),
                "nu_destino" : data["nu_cidade_destino"],
                "destino" : data["cidade_destino"],
                "ponto_chegada" : data['localizacao_final'],
                "descricao_tempo_destino" : destination_data['descricao_tempo'],
                "temp_destino" : destination_data['temperatura'],
                "sensacao_termica_destino" : destination_data['sensacao_termica'],
                "temp_minima_destino" : destination_data['temperatura_minima'],
                "temp_max_destino" : destination_data['temperatura_maxima'],
                "altitude_destino" : destination_data.get('nivel_do_mar'),
                "cd_distancia" : data['cd_distancia'],
                "distancia" : data['distancia'],
                "cd_duracao" : data['cd_duracao'],
                "duracao" : data["duracao"],
                "passos" : data['passos'],
                "dt_carga" : ts,
                "dt" : data["dt"]
            }
            
            print(formatted_data)
            new_data.append(formatted_data)
    
        return new_data