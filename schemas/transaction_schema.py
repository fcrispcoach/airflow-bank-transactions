from cerberus import Validator

TRANSACTION_SCHEMA = {
    'id': {'type': 'string', 'required': True, 'regex': r'^TRX-\d{4}$'},
    'data': {'type': 'string', 'required': True, 'regex': r'^\d{4}-\d{2}-\d{2}$'},
    'valor': {'type': ['integer', 'float'], 'required': True, 'min': 0},
    'conta_origem': {'type': 'string', 'required': True, 'regex': r'^\d{5}-\d$'},
    'conta_destino': {'type': 'string', 'required': True, 'regex': r'^\d{5}-\d$'},
    'tipo': {'type': 'string', 'required': True, 'allowed': ['transferencia', 'deposito', 'saque', 'pagamento']},
    'status': {'type': 'string', 'required': True, 'allowed': ['completo', 'pendente', 'falha']}
}

def get_validator():
    return Validator(TRANSACTION_SCHEMA)