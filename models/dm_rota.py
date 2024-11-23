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
class dmRota:
    schema: str = 'refined'
    name: str = 'dm_rota'
    key_column: str = 'nu_rota'
    order_column: str = 'dt_carga'
    
    @staticmethod
    def get_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego na trusted zone
        
        Retorno:
        StructType: Estrutura que será utilizada para montar o DataFrame
        """
        schema =  T.StructType([
            T.StructField("nu_rota", T.StringType(), False),          # Chave primária única para cada rota
            T.StructField("nu_cidade_origem", T.IntegerType(), True), # FK para `dm_cidade` (origem)
            T.StructField("cidade_origem", T.StringType(), True),
            T.StructField("nu_cidade_destino", T.IntegerType(), True),# FK para `dm_cidade` (destino)
            T.StructField("cidade_destino", T.StringType(), True),
            T.StructField("cd_distancia", T.StringType(), True),
            T.StructField("distancia", T.IntegerType(), True),
            T.StructField("dt_carga", T.DateType(), True),             # Data e hora de carga ou atualização
            T.StructField("dt", T.DateType(), True) 
        ])

        return schema