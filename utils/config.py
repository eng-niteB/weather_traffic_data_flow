# -*- coding: utf-8 -*-
import os
import sys
import shutil
from dotenv import load_dotenv

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.timer import timer_func

def load_env_variables():
    """
    Load environment variables from a .env file.
    """
    directory = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(directory, '../.env')
    load_dotenv(dotenv_path)

@timer_func
def check_if_table_exists(table_dir: str) -> bool:
    """
    Verifica se o diretório da tabela existe e testa algumas condições:
    1 - Se existir e estiver vazio, exclui o diretório e retorna False.
    2 - Se existir e não estiver vazio, retorna True.
    3 - Se não existir, retorna False.

    Argumentos:
    table_dir (str): Caminho do diretório da tabela.

    Retorno:
    bool: True se o diretório existir e não estiver vazio, False caso contrário.
    """
    try:
        if os.path.isdir(table_dir):
            if len(os.listdir(table_dir)) == 0:
                shutil.rmtree(table_dir)
                return False
            return True
        return False
    except Exception as err:
        print(f"Erro ao verificar o diretório {table_dir}: {err}")
        return False