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
class dmCidade:
    schema: str = 'refined'
    name: str = 'dm_cidade'
    key_column: str = 'nu_cidade'
    order_column: str = 'dt_carga'
    
    @staticmethod
    def get_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego na trusted zone
        
        Retorno:
        StructType: Estrutura que será utilizada para montar o DataFrame
        """
        schema = T.StructType([
                T.StructField("nu_clima", T.StringType(), False),           # Chave primária gerada (nu_cidade + dt_carga)
                T.StructField("nu_cidade", T.IntegerType(), True),          # Chave estrangeira para `dm_cidade`
                T.StructField("descricao_tempo", T.StringType(), True),
                T.StructField("temperatura", T.FloatType(), True),
                T.StructField("sensacao_termica", T.FloatType(), True),
                T.StructField("temperatura_minima", T.FloatType(), True),
                T.StructField("temperatura_maxima", T.FloatType(), True),
                T.StructField("pressao", T.IntegerType(), True),
                T.StructField("umidade", T.IntegerType(), True),
                T.StructField("visibilidade", T.IntegerType(), True),
                T.StructField("velocidade_vento", T.FloatType(), True),
                T.StructField("direcao_vento", T.IntegerType(), True),
                T.StructField("rajada_vento", T.FloatType(), True),
                T.StructField("nivel_do_mar", T.IntegerType(), True),
                T.StructField("nivel_do_solo", T.IntegerType(), True),
                T.StructField("nascer_do_sol", T.LongType(), True),
                T.StructField("por_do_sol", T.LongType(), True),
                T.StructField("dt_carga", T.LongType(), True),
                T.StructField("dt", T.StringType(), True)  
        ])
        
        return schema