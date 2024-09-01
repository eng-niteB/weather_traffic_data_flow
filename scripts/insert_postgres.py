# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json
from pyspark.sql import functions as F
import argparse

"""
Objetivo:
    Buscar os dados da trusted zone do hadoop e disponibilizar na tabela final no postgres
    
Bash uso:

"""

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.postgres import save_to_postgres
from utils.spark import get_spark_session
from utils.timer import timer_func
from utils.config import read_secret
from models.route_description import routeDescription

@timer_func
def get_args() -> argparse.Namespace:
    """
    Define os parametros passados durante a execução do código e retorna seus valores
    
    Retorno:
    argparse.Namespace: Objeto com os valores dos argumentos
    """
    parser = argparse.ArgumentParser(description="Argumentos para a carga da tabela")
    parser.add_argument('--mode', type=str, required=True, help="Indica se será um 'append' ou 'overwrite'")
    parser.add_argument('--dbOrigin', type=str, required=False, help="Base origem")
    parser.add_argument('--tbOrigin', type=str, required=False, help="Tabela origem")
    parser.add_argument('--dbDest', type=str, required=False, help="Base destino")
    parser.add_argument('--tbDest', type=str, required=False, help="Tabela destino")
    return parser.parse_args()

if __name__ == "__main__":
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = read_secret('/run/secrets/database_dir')
    
    #Coletando argumentos passados via linha de comando
    args = get_args()
    
    mode : str = args.mode
    dbOrigin : str = args.dbOrigin
    tbOrigin : str = args.tbOrigin
    dbDest : str = args.dbDest
    tbDest : str = args.tbDest

    tbDir : str = f"{database_dir}/{dbOrigin}/{tbOrigin}"
    
    #Criando sessão spark
    spark : SparkSession = get_spark_session()
    
    df : DataFrame = spark.read.parquet(tbDir)
    
    df = df.withColumn("passos", to_json(F.col("passos")))
    
    save_to_postgres(df, dbDest, tbDest, mode)