import pandas as pd
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine, TIMESTAMP, FLOAT, BOOLEAN, TEXT
import numpy as np

# Configurações
DB_URL = "postgresql://monitoramento:senha123@localhost/series_temporais"
CONTAMINATION = 0.1  # 10% de anomalias

def detect_anomalies():
    # Conectar ao banco
    engine = create_engine(DB_URL)
    
    # Carregar dados
    df = pd.read_sql("SELECT * FROM historical_data", engine)
    
    # Treinar modelo de detecção
    model = IsolationForest(
        contamination=CONTAMINATION,
        random_state=42
    )
    
    # Detectar anomalias
    scores = model.fit_predict(df[['value']])
    df['anomaly_score'] = -model.score_samples(df[['value']])  # Quanto maior, mais anômalo
    df['is_anomaly'] = (scores == -1)
    
    # Adicionar valores previstos (simulação simples)
    df['predicted_value'] = df['value'].rolling(window=3).mean().shift(1)
    
    # Adicionar explicações simuladas
    explanation_conditions = (
        df['value'] > df['predicted_value']
    ).map({
        True: lambda x: f"{((x['value']/x['predicted_value']-1)*100):.1f}% acima do esperado",
        False: lambda x: f"{((1-x['value']/x['predicted_value'])*100):.1f}% abaixo do esperado"
    })
    
    df['explanation'] = np.where(
        df['is_anomaly'],
        'Valor ' + df.apply(explanation_conditions, axis=1),
        None
    )
    
    # Salvar previsões com tipo de dados explícito
    df[['timestamp', 'predicted_value', 'anomaly_score', 'is_anomaly', 'explanation']].to_sql(
        'predictions',
        engine,
        if_exists='replace',
        index=False,
        dtype={
            'timestamp': TIMESTAMP,
            'predicted_value': FLOAT,
            'anomaly_score': FLOAT,
            'is_anomaly': BOOLEAN,
            'explanation': TEXT
        }
    )
    
    print(f"✅ {df['is_anomaly'].sum()} anomalias detectadas!")
    print("📊 Estrutura da tabela predictions atualizada com sucesso")

if __name__ == "__main__":
    detect_anomalies()