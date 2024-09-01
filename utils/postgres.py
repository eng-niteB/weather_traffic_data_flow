from pyspark.sql import DataFrame
from utils.timer import timer_func
from utils.config import read_secret

user = read_secret('/run/secrets/postgres_user')
password = read_secret('/run/secrets/postgres_password')

# Função para salvar dados no PostgreSQL
@timer_func
def save_to_postgres(df: DataFrame, schema: str, table_name: str, mode: str = "overwrite") -> None:
    dbtable = f"{schema}.{table_name}"
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres:5432/dashboards") \
      .option("dbtable", dbtable) \
      .option("user", user) \
      .option("password", password) \
      .option("driver", "org.postgresql.Driver") \
      .mode(mode) \
      .save()