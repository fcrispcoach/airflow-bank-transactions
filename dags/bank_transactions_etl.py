from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import os
from io import StringIO
from faker import Faker
import random
from sklearn.ensemble import IsolationForest
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_fake_data(**kwargs):
    """Gera dados sintéticos e adiciona ao arquivo existente (append)"""
    fake = Faker()
    num_transactions = random.randint(50, 150)  
    
    
    file_path = os.path.join(os.path.dirname(__file__), '../data/transacoes_bancarias.csv')
    
    
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
    else:
        df_existing = pd.DataFrame()

    
    new_data = []
    for _ in range(num_transactions):
        amount = round(random.uniform(-10000, 10000), 2)
        status = 'sucesso' if random.random() > 0.15 else 'falha'  
        
        new_data.append({
            'id_transacao': fake.uuid4(),
            'nome_cliente': fake.name(),
            'data': fake.date_between(start_date='-30d', end_date='today').isoformat(),
            'valor': amount,
            'status': status,
            'motivo_falha': fake.sentence() if status == 'falha' else None
        })
    
    
    df_new = pd.DataFrame(new_data)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    
    
    if len(df_combined) > 10000:
        df_combined = df_combined.tail(10000)
    
    
    df_combined.to_csv(file_path, index=False)
    print(f"Adicionados {len(df_new)} transações. Total: {len(df_combined)} registros.")

def extract_data(**kwargs):
    """Lê o CSV atualizado"""
    file_path = os.path.join(os.path.dirname(__file__), '../data/transacoes_bancarias.csv')
    df = pd.read_csv(file_path)
    df['data'] = pd.to_datetime(df['data'])  
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json(orient='records', date_format='iso'))

def transform_data(**kwargs):
    """Transforma os dados para análise"""
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract', key='raw_data')
    
    df = pd.read_json(StringIO(raw_data), orient='records')
    df['valor_absoluto'] = df['valor'].abs()
    df['tipo_transacao'] = df['valor'].apply(lambda x: 'crédito' if x > 0 else 'débito')
    df['data'] = pd.to_datetime(df['data'])  
    
    ti.xcom_push(key='transformed_data', value=df.to_json(orient='records', date_format='iso'))

def load_data(**kwargs):
    """Gera relatório consolidado"""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    df = pd.read_json(StringIO(transformed_data), orient='records')
    
    print("\nRESUMO ESTATÍSTICO:")
    print(f"• Transações totais: {len(df)}")
    print(f"• Sucesso: {len(df[df['status'] == 'sucesso'])} | Falhas: {len(df[df['status'] == 'falha'])}")
    print(f"• Valor médio (crédito): R${df[df['tipo_transacao'] == 'crédito']['valor'].mean():.2f}")
    print(f"• Valor médio (débito): R${df[df['tipo_transacao'] == 'débito']['valor'].mean():.2f}")

def run_data_quality_checks(**kwargs):
    """Valida a qualidade dos dados com verificações robustas"""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    if not transformed_data:
        raise ValueError("Nenhum dado recebido da tarefa 'transform'")
    
    try:
        df = pd.read_json(StringIO(transformed_data), orient='records')
        
        
        df['data'] = pd.to_datetime(df['data']).dt.date  
        today = datetime.now().date()
        
        checks = {
            'IDs únicos': df['id_transacao'].nunique() == len(df),
            'Sem nomes vazios': not df['nome_cliente'].isnull().any(),
            'Datas válidas': df['data'].between(pd.to_datetime('2024-01-01').date(), today).all(),
            'Status válidos': set(df['status'].unique()) == {'sucesso', 'falha'},
            'Motivos de falha': df[df['status'] == 'falha']['motivo_falha'].notna().all()
        }
        
        for test_name, passed in checks.items():
            if not passed:
                error_msg = f"Falha no teste: {test_name}"
                if test_name == 'Datas válidas':
                    invalid_dates = df[~df['data'].between(pd.to_datetime('2024-01-01').date(), today)]
                    error_msg += f"\nDatas inválidas:\n{invalid_dates[['id_transacao', 'data']]}"
                raise ValueError(error_msg)
        
        print("Todos os testes de qualidade passaram!")
        
    except Exception as e:
        raise ValueError(f"Erro durante as verificações: {str(e)}")
    
    print("Todos os testes de qualidade passaram!")

def detect_anomalies(**kwargs):
    """Identifica transações suspeitas com Isolation Forest"""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    df = pd.read_json(StringIO(transformed_data), orient='records')
    
    
    features = df[['valor_absoluto']].copy()
    features['is_failure'] = (df['status'] == 'falha').astype(int)
    
    
    model = IsolationForest(contamination=0.05, random_state=42)
    df['anomaly_score'] = model.fit_predict(features)
    df['is_anomaly'] = (df['anomaly_score'] == -1)
    
    
    anomalies = df[df['is_anomaly']]
    if not anomalies.empty:
        print("\nANOMALIAS DETECTADAS:")
        print(anomalies[['id_transacao', 'nome_cliente', 'valor', 'status']])
        
        
        anomaly_path = os.path.join(os.path.dirname(__file__), '../data/anomalias.csv')
        anomalies.to_csv(anomaly_path, index=False)
    else:
        print("\nNenhuma anomalia encontrada")

with DAG(
    'bank_transactions_ml_append',
    default_args=default_args,
    description='Pipeline com append de dados e detecção de anomalias',
    schedule_interval=timedelta(hours=6),  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['financeiro', 'ml'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    generate_data = PythonOperator(
        task_id='generate_fake_data',
        python_callable=generate_fake_data,
    )
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )
    
    data_quality = PythonOperator(
        task_id='data_quality',
        python_callable=run_data_quality_checks,
    )
    
    anomaly_detection = PythonOperator(
        task_id='detect_anomalies',
        python_callable=detect_anomalies,
    )
    
    end = EmptyOperator(task_id='end')
    
    
    start >> generate_data >> extract >> transform >> [load, data_quality, anomaly_detection] >> end