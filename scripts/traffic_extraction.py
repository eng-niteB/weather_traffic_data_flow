# -*- coding: utf-8 -*-
import os
import sys
import requests

"""
OBJETIVO: Coletar os dados da API do Maps e disponibiliza-los na camada da raw no Hadoop

"""

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!!!DEV!!!
#remover na versao final
from utils.config import load_env_variables
load_env_variables()

from utils.timer import timer_func
from utils.spark import get_spark_session,create_table,insert_data

if __name__ == "__main__":
    print('Hello')