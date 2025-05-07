import psycopg2
from datetime import datetime, timedelta
import random

# Configuração do PostgreSQL
conn = psycopg2.connect(
    dbname="time_series",
    user="postgres",
    password="sua_senha_segura",  # Use a senha que definiu
    host="localhost"
)
cursor = conn.cursor()

# Gerar dados sintéticos (ex: vendas diárias)
start_date = datetime(2023, 1, 1)
for day in range(365):
    date = start_date + timedelta(days=day)
    value = random.randint(100, 200)  # Valor base
    # Adicionar picos aleatórios (anomalias)
    if random.random() < 0.05:
        value *= 2
    cursor.execute(
        "INSERT INTO historical_data (timestamp, value, metric_name) VALUES (%s, %s, %s)",
        (date, value, "vendas_diarias")
    )

conn.commit()
cursor.close()
conn.close()
print("Dados inseridos com sucesso!")