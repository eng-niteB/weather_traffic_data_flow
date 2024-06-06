import os
import requests
from utils.config import load_env_variables
from utils.timer import timer_func
from pyspark.sql import SparkSession

load_env_variables()

@timer_func
def get_spark_session(temp_dir: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName("ETLJob") \
        .config("spark.jars", jar_path) \
        .config("spark.local.dir", temp_dir) \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

if __name__ == "__main__":
    city = 'Divinopolis'
    schema = 'raw'
    table_name = 'weather_data'
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    temp_dir = os.getenv('TEMP_DIR')
    jar_path = os.getenv('POSTGRES_JAR_PATH')

