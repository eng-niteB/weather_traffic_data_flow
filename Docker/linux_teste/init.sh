#!/bin/bash

# Copiar segredos para o diretório root e ajustar permissões
cp /run/secrets/maps_key /root/maps_key
cp /run/secrets/weather_key /root/weather_key
cp /run/secrets/temp_dir /root/temp_dir
cp /run/secrets/postgres_jar_path /root/postgres_jar_path
cp /run/secrets/postgres_jar_path /root/postgres_user
cp /run/secrets/postgres_jar_path /root/postgres_password
cp /run/secrets/database_dir /root/database_dir

# Ajustar permissões
chmod 600 /root/maps_key /root/weather_key /root/temp_dir /root/postgres_jar_path /root/database_dir

# Printar os segredos para depuração
echo "Maps Key: $(cat /root/maps_key)"
echo "Weather Key: $(cat /root/weather_key)"
echo "Temp Dir: $(cat /root/temp_dir)"
echo "Postgres JAR Path: $(cat /root/postgres_jar_path)"
echo "Database Dir: $(cat /root/database_dir)"

# Executar o comando original do contêiner
exec "$@"