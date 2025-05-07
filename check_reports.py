import pandas as pd
from pathlib import Path

reports_dir = Path("/home/pyshell/airflow/data/reports")

if not reports_dir.exists():
    print(f"ERRO: Diretório {reports_dir} não existe!")
else:
    reports = list(reports_dir.glob("fraud_report_*.csv"))
    
    if not reports:
        print("Nenhum relatório encontrado. Verifique:")
        print("1. Se a tarefa predict_new_transactions está sendo executada")
        print("2. Se há transações suspeitas sendo detectadas")
        print("3. Permissões do diretório:", reports_dir.stat())
    else:
        latest = max(reports, key=lambda x: x.stat().st_mtime)
        print(f"Último relatório: {latest}")
        print("Conteúdo:")
        print(pd.read_csv(latest).to_markdown())