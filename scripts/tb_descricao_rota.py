# -*- coding: utf-8 -*-
import os
import sys
from typing import Dict, Any, List

#Adicionar o diretório principal ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import timer_func,read_secret
from utils.spark import get_spark_session,create_table,insert_trusted_data
from models.traffic import Traffic
from models.weather import Weather
from models.route_description import routeDescription


if __name__ == "__main__":
    #Buscando o caminho do diretorio base das tabelas
    database_dir : str = read_secret('/run/secrets/database_dir')
    
    #Criando as classes a serem utilizdas
    weather : Weather = Weather()
    routes : Traffic = Traffic()
    route_description = routeDescription()
    
    key_column = route_description.key_column
    order_colum = route_description.order_column
    
    #Montando caminhos especificos da camda e da tabela
    routes_table_dir : str = f"{database_dir}/{routes.trusted_schema}/{routes.trusted_name}"
    weather_table_dir : str = f"{database_dir}/{weather.trusted_schema}/{weather.trusted_name}"
    route_description_table_dir : str = f"{database_dir}/{route_description.schema}/{route_description.name}"
    
    #Criando sessão spark
    spark =  get_spark_session()
     
    #Verificando se a tabela existe e se não criando-a
    create_table(spark,route_description_table_dir,route_description.name,route_description.get_trusted_schema())
    
    #Busca os dados de rotas solicitadas
    df_routes = spark.read.parquet(routes_table_dir)
    
    #Coletando os dados de rota
    routes = df_routes.collect()
    
    #Coletando as linhas
    datas : List[Dict[str, Any]] = [row.asDict() for row in routes]
     
    #Formatando os dados
    new_data = route_description.format_data(spark,weather_table_dir,datas)
    
    #Criando DataFrame com os novos dados
    df_new_data = spark.createDataFrame(new_data,route_description.get_trusted_schema())
    
    #Coletando os dados ja existentes na tabela
    df_trusted = spark.read.parquet(route_description_table_dir)
    
    #Inserindo os novos dados na tabela
    insert_trusted_data(df_new_data, df_trusted, key_column, order_colum, route_description_table_dir)
    
    #Encerrando a sessao do spark
    spark.stop()
    
    
        
    
        