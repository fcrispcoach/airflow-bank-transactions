from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
from sklearn.ensemble import IsolationForest
import great_expectations as ge

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def validate_data():
    engine = create_engine("postgresql://monitoramento:senha123@localhost/series_temporais")
    df = pd.read_sql("SELECT * FROM historical_data", engine)
    
    
    results = ge.from_pandas(df).expect_column_values_to_not_be_null("value")
    assert results.success, "Dados inválidos encontrados"

def train_model():
    engine = create_engine("postgresql://monitoramento:senha123@localhost/series_temporais")
    df = pd.read_sql("SELECT * FROM historical_data", engine)
    
    
    model = Prophet()
    model.fit(df.rename(columns={"timestamp": "ds", "value": "y"}))
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)
    
   
    clf = IsolationForest(contamination=0.05)
    df['anomaly'] = clf.fit_predict(df[['value']])
    
    
    forecast[['ds', 'yhat']].rename(columns={'ds': 'timestamp'}).to_sql(
        'predictions', engine, if_exists='replace', index=False)

with DAG(
    'anomaly_detection',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1)
) as dag:
    
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
    
    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
    
    validate_task >> train_task