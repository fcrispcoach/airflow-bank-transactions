import psycopg2
from datetime import datetime, timedelta
import random

conn = psycopg2.connect(
    dbname="series_temporais",
    user="monitoramento",
    password="senha123",
    host="localhost"
)

cur = conn.cursor()


cur.execute("TRUNCATE historical_data, predictions RESTART IDENTITY;")


start_date = datetime.now() - timedelta(days=365)
for i in range(365):
    date = start_date + timedelta(days=i)
    value = random.uniform(90, 110)
   
    if random.random() < 0.05:
        value *= 1.5
    cur.execute(
        "INSERT INTO historical_data (timestamp, value, metric_name) VALUES (%s, %s, %s)",
        (date, value, "vendas_diarias")
    )

conn.commit()
cur.close()
conn.close()