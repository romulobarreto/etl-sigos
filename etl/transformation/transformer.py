# transformation/transformer.py
import os
import glob
import csv
import logging
from datetime import datetime
import unicodedata
import pandas as pd

logger = logging.getLogger(__name__)

# ====
# Helpers de leitura robusta
# ====


def _sniff_delimiter(path, sample_bytes=8192):
    try:
        with open(path, 'r', encoding='latin1', errors='ignore') as f:
            sample = f.read(sample_bytes)
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except Exception:
        return None  # deixa o pandas inferir (sep=None com engine='python')

def _robust_read_csv(path, force_skip=False, known_columns=None):
    attempts = []

    # 1) Detectar separador
    detected = _sniff_delimiter(path)
    if detected:
        attempts.append(dict(sep=detected, engine="python"))
    # 2) Deixar pandas inferir
    attempts.append(dict(sep=None, engine="python"))
    # 3) Forçar semicolon
    attempts.append(dict(sep=";", engine="python"))
    # 4) Forçar comma
    attempts.append(dict(sep=",", engine="python"))
    # 5) Forçar tab
    attempts.append(dict(sep="\t", engine="python"))

    # Se pediu para pular linhas ruins, adiciona variações com on_bad_lines='skip'
    if force_skip:
        attempts = [dict(**a, on_bad_lines="skip") for a in attempts]

    last_exc = None
    for opts in attempts:
        try:
            df = pd.read_csv(
                path,
                encoding="latin1",
                dtype=str,
                keep_default_na=False,
                quoting=csv.QUOTE_MINIMAL,
                **opts
            )
            # Se o arquivo tiver "lixo" antes do header, tenta realinhar baseado em colunas conhecidas
            if known_columns:
                has_any = any(col in df.columns for col in known_columns)
                if not has_any:
                    with open(path, 'r', encoding='latin1', errors='ignore') as f:
                        lines = f.readlines()
                    header_idx = None
                    for idx, line in enumerate(lines[:100]):
                        if any(col in line for col in known_columns):
                            header_idx = idx
                            break
                    if header_idx is not None:
                        df = pd.read_csv(
                            path,
                            encoding="latin1",
                            dtype=str,
                            keep_default_na=False,
                            skiprows=header_idx,
                            **opts
                        )
            logger.info(f"Lido com sucesso: {os.path.basename(path)} usando opts={opts}")
            return df
        except Exception as e:
            last_exc = e
            logger.warning(f"Falha lendo {os.path.basename(path)} com opts={opts}: {e}")

    # Última cartada: repetir com skip
    if not force_skip:
        logger.warning(f"Tentativas padrão falharam para {os.path.basename(path)}. Repetindo com on_bad_lines='skip'.")
        return _robust_read_csv(path, force_skip=True, known_columns=known_columns)

    # Se nada deu, estoura
    raise last_exc

def _read_all_csvs(folder, pattern, known_columns=None):
    paths = sorted(glob.glob(os.path.join(folder, pattern)))
    if not paths:
        logger.warning(f"Nenhum arquivo encontrado em {folder} com pattern {pattern}")
        return []
    dfs = []
    for p in paths:
        try:
            df = _robust_read_csv(p, known_columns=known_columns)
            dfs.append(df)
        except Exception as e:
            logger.exception(f"Falha definitiva ao ler {os.path.basename(p)}: {e}")
            # Opcional: mover para quarentena
            # os.makedirs("quarentena", exist_ok=True)
            # shutil.move(p, os.path.join("quarentena", os.path.basename(p)))
    return dfs

# ====
# Helpers de normalização
# ====

def _norm_col(s: str) -> str:
    # Normaliza nome de colunas: remove acentos, trim, colapsa espaços, lower only onde útil
    if not isinstance(s, str):
        return s
    s = s.replace('"', '').strip()
    s = " ".join(s.split())
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    return s  # mantém case original por ora (para bater com nomes do SIGOS)

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    return df

def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte automaticamente todas as colunas que contenham 'DATA' no nome para datetime.date
    """
    df = df.copy()
    for col in df.columns:
        if "DATA" in col.upper():  # pega qualquer coluna com "DATA" no nome
            logger.info(f"Convertendo coluna de data: {col}")
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date
    return df

def _parse_time_series(series: pd.Series) -> pd.Series:
    # Converte HH:MM[:SS] para time; se vier vazio, vira NaT
    return pd.to_datetime(series, format="%H:%M:%S", errors="coerce").dt.time.fillna(
        pd.to_datetime(series, format="%H:%M", errors="coerce").dt.time
    )

def _parse_timestamp_series(series: pd.Series, dayfirst=True) -> pd.Series:
    # Converte timestamp completo; aceita ISO ou BR
    return pd.to_datetime(series, dayfirst=dayfirst, errors="coerce")

def _combine_date_time(df: pd.DataFrame, date_col: str, time_col: str, target_col: str) -> pd.DataFrame:
    # Se time_col tiver só hora, combina com date_col para gerar um timestamp
    if date_col in df.columns and time_col in df.columns:
        # tenta entender se time_col já é timestamp
        ts = pd.to_datetime(df[time_col], errors="coerce", dayfirst=True)
        only_time = ts.isna()  # se NaT, provavelmente era só hora
        if only_time.any():
            d = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce").dt.date
            t = _parse_time_series(df[time_col])
            combined = pd.to_datetime(d.astype(str) + " " + pd.Series(t).astype(str), errors="coerce")
            # onde time_col já era timestamp, usa ele
            ts = ts.fillna(combined)
        df[target_col] = ts
    return df

def _add_audit_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["data_extracao"] = datetime.now()  # timestamp do ETL
    return df

def _drop_cols_safe(df: pd.DataFrame, cols_to_drop: list[str]) -> pd.DataFrame:
    df = df.copy()
    existing = [c for c in cols_to_drop if c in df.columns]
    if existing:
        df = df.drop(columns=existing)
    return df

def _deduplicate_df(df: pd.DataFrame, subset_keys: list[str]) -> pd.DataFrame:
    """
    Remove duplicatas mantendo a versão mais recente (baseado em data_extracao).
    """
    # Filtra apenas as chaves que existem no DataFrame
    existing_keys = [k for k in subset_keys if k in df.columns]
    
    if not existing_keys:
        logger.warning(f"Nenhuma chave de deduplicação encontrada: {subset_keys}")
        return df
    
    initial_count = len(df)
    
    # Ordena por data_extracao (mais antigo -> mais novo) e remove duplicatas mantendo o último
    df_dedup = df.sort_values("data_extracao").drop_duplicates(subset=existing_keys, keep="last")
    
    final_count = len(df_dedup)
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.info(f"Deduplicação: removidas {removed_count} linhas duplicadas de {initial_count} (chaves: {existing_keys})")
    else:
        logger.info(f"Deduplicação: nenhuma duplicata encontrada em {initial_count} linhas")
    
    return df_dedup

# ====
# Transformadores principais
# ====

def transformar_return(mode: str) -> pd.DataFrame:
    """
    Lê todos os arquivos de retorno*.csv em downloads/, normaliza e retorna um único DataFrame.
    """
    known_cols = ["REGIONAL", "UC / MD", "TIPO SERVICO", "DATA EXECUCAO", "CODIGO"]
    dfs = _read_all_csvs("downloads", "retorno*.csv", known_columns=known_cols)
    if not dfs:
        raise FileNotFoundError("Nenhum CSV de retorno encontrado para processar.")

    df = pd.concat(dfs, ignore_index=True)
    df = _normalize_columns(df)
    # Converte automaticamente todas as colunas de DATA
    df = _normalize_date_columns(df)

    # Padroniza nome da coluna de data de execução
    if "DATA EXECUCAO" in df.columns:
        df = df.rename(columns={"DATA EXECUCAO": "data_execucao"})
    elif "Data execucao" in df.columns:
        df = df.rename(columns={"Data execucao": "data_execucao"})

    # Listas para você ajustar ao seu layout
    cols_para_remover = ['REGIONAL', 'TIPO SERVICO', 'FISCAL', 'EMPRESA', 'DATA ENTREGA', 'RETORNO DE']

    # Ajustes finais
    df = _drop_cols_safe(df, cols_para_remover)

    # Auditoria
    df = _add_audit_cols(df)

    # Deduplicação - usa UC/MD + Data execucao + Cod como chaves principais
    dedup_keys = ["UC / MD", "data_execucao", "CODIGO", "TOI", "EQUIPE"]
    df = _deduplicate_df(df, dedup_keys)

    return df


def transformar_general(mode: str) -> pd.DataFrame:
    """
    Lê todos os arquivos relatorio_prot_geral*.csv em downloads/, normaliza e retorna um único DataFrame.
    - Hora -> TIMESTAMP (preferência por combinar com Data lancado ou Data execucao)
    - Hora inicio servico / Hora fim servico -> TIME
    """
    known_cols = ["UC / MD", "Status", "Motivo nao baixado"]  # ajuste se necessário ao layout do "general"
    dfs = _read_all_csvs("downloads", "relatorio_prot_geral*.csv", known_columns=known_cols)
    if not dfs:
        raise FileNotFoundError("Nenhum CSV 'relatorio_prot_geral*.csv' encontrado para processar.")

    df = pd.concat(dfs, ignore_index=True)
    df = _normalize_columns(df)
    # Converte automaticamente todas as colunas de DATA
    df = _normalize_date_columns(df)

    # Padroniza nome da coluna de data de execução
    if "DATA EXECUCAO" in df.columns:
        df = df.rename(columns={"DATA EXECUCAO": "data_execucao"})
    elif "Data execucao" in df.columns:
        df = df.rename(columns={"Data execucao": "data_execucao"})

    cols_para_remover = ['Motivo nao baixado', 'Regional', 'Empresa', 'Sit deixada', 'Fiscal', 'tipo_servico_comercial', 'obs_at', 'RS Entrada', 'Lancado por', 'Data lancado', "Hora"]

    # Times
    for tcol in ["Hora inicio servico", "Hora fim servico"]:
        if tcol in df.columns:
            df[tcol] = _parse_time_series(df[tcol])


    df = _drop_cols_safe(df, cols_para_remover)
    df = _add_audit_cols(df)

    # Deduplicação
    dedup_keys = ["UC / MD", "data_execucao", "Cod", "TOI", "Equipe"]
    df = _deduplicate_df(df, dedup_keys)

    return df