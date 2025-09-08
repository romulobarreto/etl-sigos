import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, DateTime, Time
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def init_database():
    engine = get_engine()
    sql_path = os.path.join("etl", "sql", "init_tables.sql")  # Ajustado para local, não /app/
    if os.path.exists(sql_path):
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
        with engine.begin() as conn:
            conn.execute(text(sql_content))
        print("[INIT] Tabelas inicializadas com sucesso")
    else:
        print("[WARN] sql/init_tables.sql não encontrado")

def _dtype_map_for_table(tabela: str):
    if tabela == "return_reports":
        return {
            "data_execucao": Date(),
            "DATA RESOLVIDO": Date(),
            "data_extracao": DateTime()
        }
    if tabela == "general_reports":
        return {
            "data_execucao": Date(),
            "Data Afericao": Date(),
            "Data AR": Date(),
            "Data baixado": Date(),
            "Hora inicio servico": Time(),
            "Hora fim servico": Time(),
            "data_extracao": DateTime()
        }
    return {}

def load_df_to_postgres(df: pd.DataFrame, tabela: str, mode: str, coluna_data_execucao: str):
    engine = get_engine()

    if coluna_data_execucao not in df.columns:
        raise ValueError(f"Coluna '{coluna_data_execucao}' não encontrada no DataFrame.")

    with engine.begin() as conn:
        if mode == "full":
            conn.execute(text(f"DELETE FROM {tabela}"))
            print(f"[LOAD] Tabela {tabela} limpa completamente (modo FULL)")
        elif mode == "incremental":
            menor_data = df[coluna_data_execucao].min()
            if pd.notna(menor_data):
                conn.execute(
                    text(f'DELETE FROM {tabela} WHERE "{coluna_data_execucao}" >= :menor_data'),
                    {"menor_data": menor_data}
                )
                print(f"[LOAD] Deletados registros >= {menor_data} da tabela {tabela}")

    # Insere com types corretos
    dtype_map = _dtype_map_for_table(tabela)
    df.to_sql(
        tabela, engine, if_exists="append", index=False, dtype=dtype_map, method="multi", chunksize=1000
    )
    print(f"[LOAD] {len(df)} registros inseridos em {tabela} (modo={mode.upper()})")