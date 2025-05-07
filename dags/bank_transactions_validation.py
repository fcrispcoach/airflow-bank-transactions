import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from plugins.custom_operators import JsonValidatorOperator
from config.paths import find_json_files

def generate_report(**context):
    ti = context['ti']
    error_data = ti.xcom_pull(key='validation_errors', task_ids=context['task_id'])
    
    if error_data:
        print(f"\nRelatório para {error_data['file']}")
        print(f"Total de erros: {error_data['total_errors']}")
        
        
        report_path = f"/tmp/{os.path.basename(error_data['file'])}_errors.txt"
        with open(report_path, 'w') as f:
            f.write(f"Arquivo: {error_data['file']}\n")
            f.write(f"Total de erros: {error_data['total_errors']}\n\n")
            for err in error_data['all_errors']:
                f.write(f"Linha {err['line']}:\n")
                f.write(f"Erros: {err['errors']}\n")
                f.write(f"Dados: {err['data']}\n\n")
        
        print(f"Relatório salvo em: {report_path}")
        print("Primeiros 3 erros:")
        for err in error_data['sample_errors'][:3]:
            print(f"Linha {err['line']}: {err['errors']}")
    else:
        print(f"{context['file_path']} válido")

default_args = {
    'owner': 'airflow',
    'retries': 0,  
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bank_transactions_validation',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['validation'],
) as dag:

    for file_path in find_json_files():
        task_id = f"validate_{os.path.basename(file_path).replace('.', '_')}"
        
        validate_task = JsonValidatorOperator(
            task_id=task_id,
            file_path=file_path,
            dag=dag
        )
        
        report_task = PythonOperator(
            task_id=f"report_{task_id}",
            python_callable=generate_report,
            op_kwargs={
                'file_path': file_path,
                'task_id': task_id
            },
            trigger_rule=TriggerRule.ALL_DONE,  
            dag=dag
        )
        
        validate_task >> report_task