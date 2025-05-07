from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from schemas.transaction_schema import get_validator
import json

class JsonValidatorOperator(BaseOperator):
    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def execute(self, context):
        validator = get_validator()
        errors = []
        
        with open(self.file_path) as f:
            transactions = json.load(f)
        
        for idx, transaction in enumerate(transactions):
            if not validator.validate(transaction):
                errors.append({
                    'line': idx + 1,
                    'errors': validator.errors,
                    'data': transaction
                })
        
        if errors:
            
            context['ti'].xcom_push(key='validation_errors', value={
                'file': self.file_path,
                'total_errors': len(errors),
                'sample_errors': errors[:10],  
                'all_errors': errors  
            })
            
            self.log.warning(f"Erros em {self.file_path}: {len(errors)} inválidas")
            for err in errors[:3]:  
                self.log.warning(f"Linha {err['line']}: {err['errors']}")
        
        return len(errors)  