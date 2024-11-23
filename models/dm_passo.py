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
class fpPasso:
    schema: str = 'refined'
    name: str = 'fp_passo'
    key_column: str = 'nu_passo'
    order_column: str = None
    
    @staticmethod
    def get_schema() -> T.StructType:
        """
        Descreve e retorna a estrutura de dados esperada para a tabela de tráfego na trusted zone
        
        Retorno:
        StructType: Estrutura que será utilizada para montar o DataFrame
        """
        schema = T.StructType([
            T.StructField("nu_passo", T.StringType(), False),      # Identificador único para a passo
            T.StructField("nu_cidade_origem", T.IntegerType(), True),   # Chave estrangeira para `dm_cidade`
            T.StructField("nu_cidade_destino", T.IntegerType(), True),  # Chave estrangeira para `dm_cidade`
            T.StructField("passos", T.ArrayType(T.MapType(T.StringType(), T.StringType())), True),  # Passos detalhados da passo
            T.StructField("dt_carga", T.DateType(), True),             # Data e hora de carga ou atualização
            T.StructField("dt", T.DateType(), True) 
        ])

        return schema