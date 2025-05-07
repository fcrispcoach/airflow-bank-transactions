CREATE TABLE IF NOT EXISTS historical_data (
    timestamp TIMESTAMP PRIMARY KEY,
    value FLOAT NOT NULL,
    metric_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS predictions (
    timestamp TIMESTAMP PRIMARY KEY,
    predicted_value FLOAT NOT NULL,
    anomaly_score FLOAT NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    explanation TEXT
);