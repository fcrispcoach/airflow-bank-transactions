from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(self, checks, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
    
    def execute(self, context):
        ti = context['ti']
        data = ti.xcom_pull(task_ids='transform', key='transformed_data')
        df = pd.read_json(data)
        
        self.log.info("Executando verificações de qualidade dos dados")
        
        for check in self.checks:
            test_name = check['test_name']
            test_condition = check['condition']
            
            if not test_condition(df):
                error_msg = f"Falha no teste de qualidade: {test_name}"
                self.log.error(error_msg)
                raise ValueError(error_msg)
        
        self.log.info("Todos os testes de qualidade dos dados passaram com sucesso!")