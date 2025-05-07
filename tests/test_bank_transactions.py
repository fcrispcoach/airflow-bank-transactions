import pytest
import pandas as pd
import os
from datetime import datetime

def test_data_quality():
    
    file_path = os.path.join(os.path.dirname(__file__), '../data/transacoes_bancarias.csv')
    df = pd.read_csv(file_path)
    
    # Teste 1: Verifica se não há IDs nulos
    assert not df['id_transacao'].isnull().any(), "Existem IDs de transação nulos"
    
    # Teste 2: Verifica se não há nomes de clientes vazios
    assert not df['nome_cliente'].isnull().any(), "Existem nomes de clientes vazios"
    
    # Teste 3: Verifica se as datas estão dentro do intervalo esperado
    df['data'] = pd.to_datetime(df['data'])
    assert df['data'].between('2025-01-01', '2025-12-31').all(), "Existem datas fora do intervalo 2025"
    
    # Teste 4: Verifica se os status são apenas 'sucesso' ou 'falha'
    assert set(df['status'].unique()).issubset({'sucesso', 'falha'}), "Status inválidos encontrados"
    
    # Teste 5: Verifica se os valores estão dentro de um intervalo razoável
    assert (df['valor'].abs() <= 10000).all(), "Existem valores fora do intervalo esperado (-10.000 a 10.000)"

def test_transaction_types():
    file_path = os.path.join(os.path.dirname(__file__), '../data/transacoes_bancarias.csv')
    df = pd.read_csv(file_path)
    
    # Cria coluna de tipo de transação para teste
    df['tipo_transacao'] = df['valor'].apply(lambda x: 'credito' if x > 0 else 'debito')
    
    # Verifica se há transações de ambos os tipos
    assert 'credito' in df['tipo_transacao'].unique(), "Nenhuma transação de crédito encontrada"
    assert 'debito' in df['tipo_transacao'].unique(), "Nenhuma transação de débito encontrada"

def test_failure_reasons():
    file_path = os.path.join(os.path.dirname(__file__), '../data/transacoes_bancarias.csv')
    df = pd.read_csv(file_path)
    
    # Verifica se todas as falhas têm motivo
    falhas = df[df['status'] == 'falha']
    assert not falhas['motivo_falha'].isnull().any(), "Existem falhas sem motivo especificado"