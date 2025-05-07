import os


DATA_DIR = os.path.expanduser("~/airflow/data/dados_transacoes")


def find_json_files():
    json_files = []
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files