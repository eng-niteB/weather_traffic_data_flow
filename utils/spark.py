# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.timer import timer_func
from utils.config import check_if_table_exists
from pyspark.sql import types as T

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@timer_func
def get_spark_session(jar_path: str, temp_dir: str) -> SparkSession:
    """
    Retorna a sessao de spark configurada
    
    Argumentos:
    
    jar_path (str): Indica o caminho dos arquivos jars no ambiente
    temp_dir (str): Indica o diretorio para os arquivos temporarios do spark
    
    Retorno:
    
    SparkSession: Sessão do spark que será utilizada no código
    """
    spark = SparkSession.builder \
        .appName("ETLJob") \
        .config("spark.jars", jar_path) \
        .config("spark.local.dir", temp_dir) \
        .config("spark.sql.warehouse.dir", "/tmp/warehouse") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.nativeio.impl.disable.cache", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

@timer_func
def create_table(spark : SparkSession,table_dir: str, table_name: str, schema : T.StructType, partition_column : str) -> None:
    """
    Cria uma tabela como um arquivo parquet para simular um 'CREATE TABLE' no hadoop
    
    Argumentos:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    dir (str): diretório destino da tabela incluindo schema + nome da tabela
    table_name (str): Nome da tabela sendo criada
    schema (StructType): Estrutura da tabela destino incluindo colunas e tipos
    partition_column (str): coluna de particionamento da tabela
    """
    #Criando dados da carga inicial
    if not check_if_table_exists(table_dir):
        num_columns = len(schema.fields)
        initial_data = [(None,) * num_columns]
        
        empty_df = spark.createDataFrame(initial_data, schema)    
        empty_df.write.partitionBy(partition_column).parquet(table_dir)
    
    else:
        print(f'Tabela {table_name} já existe no ambiente')
        
@timer_func
def remove_duplicates(df: DataFrame, key_column: str, order_column : str) -> DataFrame:
    """
    Remove linhas duplicadas do dataframe
    
    
    Parametros:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    df (DataFrame): DataFrame de onde serão retiradas as duplicidades
    key_column (str): Coluna chave primaria da tabela
    order_column (str): Coluna de ordenação da tabela    
    
    Retorno:
    DataFrame : DataFrame sem duplicadas
    """
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc())
    
    df = df.withColumn('row_num', F.row_number().over(window))
    df = df.filter(F.col('row_num') == 1)
    df = df.drop('row_num')
    
    return df

@timer_func
def insert_data(spark : SparkSession,table_dir: str, table_name: str, schema : T.StructType, key_column: str, partition_column : str, order_column : str, dt : str, new_data) -> None:
    """
    Insere os dados novos na tabela destino, tratando eles antes da insercao
    
    Argumentos:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    dir (str): diretório destino da tabela incluindo schema + nome da tabela
    table_name (str): Nome da tabela sendo criada
    schema (StructType): Estrutura da tabela destino incluindo colunas e tipos
    key_column (str): Coluna chave primaria da tabela
    partition_column (str): coluna de particionamento da tabela
    order_column (str): coluna de ordenacao da tabela
    dt (str): data referencia da carga
    new_data (Dict[str, Any]): Dados novos a serem inseridos na tabela
    """
        
    df_insert = spark.createDataFrame([],schema)
    df_table = spark.read.parquet(table_dir)
    df_table = df_table.filter(F.col('id').isNotNull())
    df_destiny = df_table.filter(F.col('dt') == dt)            

    df_insert = spark.createDataFrame(new_data,schema)
    df_destiny = df_destiny.unionByName(df_insert)
    df_destiny = remove_duplicates(df_destiny, key_column, order_column)
    df_destiny.write.mode("overwrite").partitionBy(partition_column).parquet(table_dir)
    print(f'Dado inserido na tabela {table_name} para a partição {dt}')