import pandas as pd
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine
import numpy as np

# Configurações
DB_URL = "postgresql://monitoramento:senha123@localhost/series_temporais"
CONTAMINATION = 0.1  # 10% de anomalias (ajuste conforme necessário)

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
    df['is_anomaly'] = scores == -1
    
    # Adicionar explicações simuladas
    df['explanation'] = np.where(
        df['is_anomaly'],
        'Valor extremo: ' + df['value'].astype(str),
        None
    )
    
    # Salvar previsões
    df[['timestamp', 'anomaly_score', 'is_anomaly', 'explanation']].to_sql(
        'predictions',
        engine,
        if_exists='replace',
        index=False
    )
    
    print(f"✅ {df['is_anomaly'].sum()} anomalias detectadas!")

if __name__ == "__main__":
    detect_anomalies()