# Monitoramento de Séries Temporais e Transações Bancárias

## Objetivo
Pipeline automatizado para:
- Monitorar séries temporais (vendas, tráfego, etc.) e transações bancárias
- Validar dados com regras rigorosas (Great Expectations + Cerberus)
- Detectar anomalias usando Machine Learning (Isolation Forest)
- Visualizar resultados em dashboards interativos
- Garantir qualidade dos dados antes do processamento

## Tecnologias
- **Orquestração**: Apache Airflow
- **Processamento**: Pandas
- **ML**: Scikit-learn (Isolation Forest), Prophet (para previsão de séries temporais)
- **Dados sintéticos**: Faker
- **Validação** : Cerberus
- **Armazenamento** :	PostgreSQL
- **Visualização**: Streamlit
- **Dados Sintéticos**:	Faker (transações), Scripts customizados (séries temporais)

Funcionalidades Integradas
1. Para Séries Temporais
Geração de dados sintéticos com anomalias controladas
Validação de qualidade com Great Expectations
Detecção de anomalias usando Isolation Forest
Dashboard interativo com Streamlit

2. Para Transações Bancárias
Simulação de transações realistas com Faker
Validação de schema com Cerberus
Detecção de fraudes com Isolation Forest
Integração com o mesmo pipeline Airflow

## Como Executar

# Clone o repositório
git clone [seu-repo]
cd monitoring-pipeline

# Instale as dependências
pip install -r requirements.txt

# Configure o banco de dados
psql -U postgres -c "CREATE DATABASE financial_monitoring;"

# Inicie o Airflow
airflow db init
airflow webserver --port 8080 & airflow scheduler

# Popule com dados iniciais
python data/populate_series.py
python data/populate_transactions.py

# Execute o dashboard
streamlit run dashboard/app.py