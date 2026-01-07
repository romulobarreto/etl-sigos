import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.types import Date, DateTime, Time
from dotenv import load_dotenv
from urllib.parse import quote_plus
from tqdm import tqdm

# Carrega .env da raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(BASE_DIR, ".env"))


# Engine global com SSL e pre_ping
def get_engine():
    user = os.getenv("DB_USER")
    password = quote_plus(os.getenv("DB_PASS"))
    db = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(
        url,
        connect_args={"sslmode": "require"},
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
    )
    return engine


def init_database():
    engine = get_engine()
    # usa caminho absoluto pro init.sql
    sql_path = os.path.join(BASE_DIR, "etl", "sql", "init_tables.sql")
    if os.path.exists(sql_path):
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
        with engine.begin() as conn:
            conn.execute(text(sql_content))
        print("[INIT] Tabelas inicializadas com sucesso")
    else:
        print("[WARN] etl/sql/init_tables.sql não encontrado")


def _dtype_map_for_table(tabela: str):
    if tabela == "return_reports":
        return {
            "DATA_EXECUCAO": Date(),
            "DATA RESOLVIDO": Date(),
            "DATA_EXTRACAO": DateTime(),
        }
    if tabela == "general_reports":
        return {
            "DATA_EXECUCAO": Date(),
            "DATA AFERICAO": Date(),
            "DATA AR": Date(),
            "DATA BAIXADO": Date(),
            "HORA INICIO SERVICO": Time(),
            "HORA FIM SERVICO": Time(),
            "DATA_EXTRACAO": DateTime(),
        }
    return {}


def _sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Converte NaN/NaT para None (Postgres aceita nulos)
    df = df.where(pd.notnull(df), None)
    # Em colunas object, garante que valores não-string viáveis virem string
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(
                lambda v: v if (v is None or isinstance(v, str)) else str(v)
            )
    return df


def load_df_to_postgres(
    df: pd.DataFrame,
    tabela: str,
    mode: str,
    coluna_data_execucao: str,
    chunksize: int = 3000,
):
    engine = get_engine()

    if coluna_data_execucao not in df.columns:
        raise ValueError(
            f"Coluna '{coluna_data_execucao}' não encontrada no DataFrame."
        )

    df = _sanitize_df(df)
    dtype_map = _dtype_map_for_table(tabela)

    # Limpeza incremental/full em transação separada
    with engine.begin() as conn:
        if mode == "full":
            print(f"[LOAD] Truncando tabela {tabela} (modo FULL)...")
            conn.execute(text(f'TRUNCATE TABLE "{tabela}" RESTART IDENTITY;'))
            print(f"[LOAD] Tabela {tabela} truncada (modo FULL)")
        elif mode == "incremental":
            menor_data = df[coluna_data_execucao].min()
            if pd.notna(menor_data):
                print(
                    f"[LOAD] Deletando registros >= {menor_data} da tabela {tabela}..."
                )
                conn.execute(
                    text(
                        f'DELETE FROM "{tabela}" WHERE "{coluna_data_execucao}" >= :menor_data'
                    ),
                    {"menor_data": menor_data},
                )
                print(
                    f"[LOAD] Deletados registros >= {menor_data} da tabela {tabela}"
                )

    # Insert com retry em erros operacionais e dump de chunks problemáticos
    total = len(df)
    attempt = 0
    max_retries = 2

    while attempt <= max_retries:
        try:
            start = 0
            with engine.begin() as conn:
                if total <= chunksize:
                    # DataFrame pequeno: insere tudo de uma vez com barra de progresso
                    print(f"[LOAD] Inserindo {total} registros em {tabela}...")
                    with tqdm(
                        total=total,
                        desc=f"Inserindo em {tabela}",
                        unit="linhas",
                    ) as pbar:
                        df.to_sql(
                            tabela,
                            con=conn,
                            if_exists="append",
                            index=False,
                            dtype=dtype_map,
                            method="multi",
                            chunksize=chunksize,
                        )
                        pbar.update(total)
                else:
                    # DataFrame grande: insere em chunks com barra de progresso
                    print(
                        f"[LOAD] Inserindo {total} registros em {tabela} (chunks de {chunksize})..."
                    )
                    with tqdm(
                        total=total,
                        desc=f"Inserindo em {tabela}",
                        unit="linhas",
                    ) as pbar:
                        while start < total:
                            end = min(start + chunksize, total)
                            chunk = df.iloc[start:end]
                            try:
                                chunk.to_sql(
                                    tabela,
                                    con=conn,
                                    if_exists="append",
                                    index=False,
                                    dtype=dtype_map,
                                    method="multi",
                                )
                                pbar.update(len(chunk))
                            except Exception as e:
                                # salva o chunk problemático pra investigar schema/dados
                                os.makedirs(
                                    os.path.join(BASE_DIR, "logs"), exist_ok=True
                                )
                                bad_path = os.path.join(
                                    BASE_DIR,
                                    "logs",
                                    f"bad_chunk_{tabela}_{start}_{end}.csv",
                                )
                                chunk.to_csv(bad_path, index=False, encoding="utf-8")
                                logging.exception(
                                    f"Falha inserindo chunk {start}:{end} em {tabela}. Salvo em {bad_path}"
                                )
                                raise
                            start = end
            print(f"[LOAD] {total} registros inseridos em {tabela} (modo={mode.upper()})")
            break  # sucesso
        except OperationalError as e:
            attempt += 1
            logging.warning(
                f"OperationalError no load ({attempt}/{max_retries}). Vou reciclar a engine e tentar de novo: {e}"
            )
            try:
                engine.dispose()
            except Exception:
                pass
            time.sleep(2)
            if attempt > max_retries:
                raise
        except SQLAlchemyError:
            logging.exception(
                "Erro SQL durante o load. Rollback automático realizado."
            )
            raise
        except Exception:
            logging.exception(
                "Erro inesperado durante o load. Rollback automático realizado."
            )
            raise