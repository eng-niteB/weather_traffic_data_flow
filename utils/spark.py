# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.timer import timer_func
from utils.config import check_if_table_exists,remove_default_partition,load_env_variables
from pyspark.sql import types as T
from typing import Optional

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_env_variables()

temp_dir : str = os.getenv('TEMP_DIR')
jar_path : str = os.getenv('POSTGRES_JAR_PATH')

@timer_func
def get_spark_session() -> SparkSession:
    """
    Retorna a sessao de spark configurada
    
    Argumentos:
    
    jar_path (str): Indica o caminho dos arquivos jars no ambiente
    temp_dir (str): Indica o diretorio para os arquivos temporarios do spark
    
    Retorno:
    
    SparkSession: Sessão do spark que será utilizada no código
    """
    spark = SparkSession.builder \
        .config("spark.jars", jar_path) \
        .config("spark.local.dir", temp_dir) \
        .config("spark.sql.warehouse.dir", "/tmp/warehouse") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.nativeio.impl.disable.cache", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.mergeSchema", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.files.minPartitionNum", "4") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("parquet.block.size", str(256 * 1024 * 1024)) \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

@timer_func
def create_table(spark : SparkSession,table_dir: str, table_name: str, schema : T.StructType, partition_column : Optional[str] = None) -> None:
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
        if partition_column is not None:
            empty_df.write.partitionBy(partition_column).parquet(table_dir)
        else:
            empty_df.write.parquet(table_dir)
    
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
@timer_func
def insert_data(spark: SparkSession, table_dir: str, table_name: str, schema: T.StructType, key_column: str, order_column: str, dt: str, new_data, partition_column: str) -> None:
    """
    Insere os dados novos na tabela destino, tratando eles antes da inserção
    
    Argumentos:
    spark (SparkSession): Sessão do spark que será utilizada no processo
    table_dir (str): Diretório destino da tabela incluindo schema + nome da tabela
    table_name (str): Nome da tabela sendo criada
    schema (StructType): Estrutura da tabela destino incluindo colunas e tipos
    key_column (str): Coluna chave primaria da tabela
    partition_column (str): Coluna de particionamento da tabela
    order_column (str): Coluna de ordenação da tabela
    dt (str): Data referência da carga
    new_data (Dict[str, Any]): Dados novos a serem inseridos na tabela
    """
    df_table = spark.read.parquet(table_dir)
    df_table = df_table.filter(F.col(key_column).isNotNull())
    df_destiny = df_table.filter(F.col('dt') == dt)

    df_insert = spark.createDataFrame(new_data, schema)
    df_insert = df_insert.filter(F.col(key_column).isNotNull())
    
    df_destiny = df_destiny.unionByName(df_insert)
    df_destiny = remove_duplicates(df_destiny, key_column, order_column)
    
    df_destiny.write.mode("overwrite").partitionBy(partition_column).parquet(table_dir)
    
    # Remover a partição padrão depois de inserir novos dados
    remove_default_partition(table_dir, partition_column)
    
    print(f'Dado inserido na tabela {table_name} para a partição {dt}')


@timer_func
def adjust_origin_column_name(df : DataFrame, schema_origin: T.StructType, schema_destiny: T.StructType) -> DataFrame:
    """
    Renomeia as colunas da raw para os nomes traduzidos da trusted
    
    Parametros:
    df_raw (DataFrame): DataFrame com as colunas a serem renomeadas
    schema_origin (StructType): Esquema das colunas da tabela da raw
    schema_destiny (StructType): Esquema das tabelas da trusted
    
    Retorno:
    DataFrame: DataFrame com as colunas renomeadas
    schema_
    """
    origin_fields = schema_origin.fields
    destiny_fields = schema_destiny.fields
    column_mapping = {origin_field.name : destiny_field.name for origin_field, destiny_field in zip(origin_fields,destiny_fields)}
    
    for origin_name, destiny_name in column_mapping.items():
        df = df.withColumnRenamed(origin_name, destiny_name)
        
    return df