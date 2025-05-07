import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# Configuração do banco de dados
engine = create_engine("postgresql://monitoramento:senha123@localhost/series_temporais")

st.title("Monitoramento de Séries Temporais")

# Dados históricos
st.header("Dados Históricos")
df = pd.read_sql("SELECT * FROM historical_data ORDER BY timestamp", engine)
fig = px.line(df, x='timestamp', y='value', title='Série Temporal')
st.plotly_chart(fig)

# Anomalias detectadas
st.header("Anomalias Detectadas")
anomalies = pd.read_sql(
    """SELECT h.timestamp, h.value as actual_value, 
       p.anomaly_score, p.explanation
       FROM historical_data h
       JOIN predictions p ON h.timestamp = p.timestamp
       WHERE p.is_anomaly = TRUE
       ORDER BY p.anomaly_score DESC""", 
    engine
)
st.dataframe(anomalies)

# Gráfico de anomalias
if not anomalies.empty:
    st.metric("Total de Anomalias", len(anomalies))
    
    tab1, tab2 = st.tabs(["Tabela", "Gráfico"])
    
    with tab1:
        st.dataframe(anomalies.sort_values('anomaly_score', ascending=False))
    
    with tab2:
        fig = px.scatter(
            anomalies,
            x='timestamp',
            y='actual_value',
            size='anomaly_score',
            color='anomaly_score',
            hover_data=['explanation'],
            title='Anomalias Detectadas'
        )
        st.plotly_chart(fig)