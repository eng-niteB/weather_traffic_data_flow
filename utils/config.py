# -*- coding: utf-8 -*-
import os
import sys
import shutil
from dotenv import load_dotenv

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.timer import timer_func

def read_secret(secret_path):
    try:
        with open(secret_path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Secret file {secret_path} not found")
        return None

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
    
def remove_default_partition(table_dir: str, partition_column: str) -> None:
    """
    Remove a partição padrão criada manualmente para simular um 'CREATE TABLE' no Hadoop
    
    Argumentos:
    table_dir (str): Diretório da tabela
    partition_column (str): Coluna de particionamento da tabela
    """
    default_partition_dir = os.path.join(table_dir, f"{partition_column}=__HIVE_DEFAULT_PARTITION__")
    
    if os.path.exists(default_partition_dir):
        shutil.rmtree(default_partition_dir)
        print(f"Partição padrão '{default_partition_dir}' removida.")
    else:
        print(f"Nenhuma partição padrão '{default_partition_dir}' encontrada para remover.")