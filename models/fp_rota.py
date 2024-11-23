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
class fpRota:
    schema: str = 'refined'
    name: str = 'fp_rota'
    key_column: str = 'nu_rota'
    order_column: str = None
    
    @staticmethod
    def get_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego na trusted zone
        
        Retorno:
        StructType: Estrutura que será utilizada para montar o DataFrame
        """
        schema = T.StructType([
            T.StructField("nu_rota", T.StringType(), False),         # FK para `dm_rota`
            T.StructField("nu_cidade_origem", T.IntegerType(), True),# FK para `dm_cidade` (origem)
            T.StructField("nu_cidade_destino", T.IntegerType(), True),# FK para `dm_cidade` (destino)
            T.StructField("nu_clima_origem", T.IntegerType(), True), # FK para `dm_clima` (origem)
            T.StructField("nu_clima_destino", T.IntegerType(), True),# FK para `dm_clima` (destino)
            T.StructField("nu_passo", T.IntegerType(), True),        # FK para `dm_passo`
            T.StructField("duracao", T.IntegerType(), True),         # Duração medida em segundos (varia por trajeto)
            T.StructField("dt", T.DateType(), True)                  # Data do trajeto
        ])

        return schema