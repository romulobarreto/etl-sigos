# transformation/transformer.py
import os
import glob
import csv
import logging
from datetime import datetime
import unicodedata
import pandas as pd

logger = logging.getLogger(__name__)

# Bases de caminho
DOWNLOADS_DIR = os.path.join(os.getcwd(), "etl", "downloads")

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
    return dfs

# ====
# Helpers de normalização
# ====

def _norm_col(s: str) -> str:
    # Normaliza nome de colunas: remove acentos, trim, colapsa espaços
    if not isinstance(s, str):
        return s
    s = s.replace('"', '').strip()
    s = " ".join(s.split())
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    return s

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
        if "DATA" in col.upper():
            logger.info(f"Convertendo coluna de data: {col}")
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date
    return df

def _parse_time_series(series: pd.Series) -> pd.Series:
    # Converte HH:MM[:SS] para time; se vier vazio, vira NaT
    return pd.to_datetime(series, format="%H:%M:%S", errors="coerce").dt.time.fillna(
        pd.to_datetime(series, format="%H:%M", errors="coerce").dt.time
    )

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

def _add_regional_grupo(df: pd.DataFrame, equipe_col: str) -> pd.DataFrame:
    """
    Adiciona colunas REGIONAL e GRUPO baseadas na coluna de equipe
    """
    if equipe_col in df.columns:
        df["REGIONAL"] = df[equipe_col].apply(lambda x: "SUL" if isinstance(x, str) and "PEL" in x else "NORTE")
        df["GRUPO"] = df[equipe_col].apply(lambda x: "AT" if isinstance(x, str) and "A0" in x else "BT")
    return df

# ====
# Transformadores principais
# ====

def transformar_return(mode: str) -> pd.DataFrame:
    """
    Lê todos os arquivos de retorno*.csv em downloads/, normaliza e retorna um único DataFrame.
    """
    known_cols = ["REGIONAL", "UC / MD", "TIPO SERVICO", "DATA EXECUCAO", "CODIGO"]
    dfs = _read_all_csvs(DOWNLOADS_DIR, "retorno*.csv", known_columns=known_cols)
    if not dfs:
        raise FileNotFoundError("Nenhum CSV de retorno encontrado para processar.")

    df = pd.concat(dfs, ignore_index=True)
    df = _normalize_columns(df)
    df = _normalize_date_columns(df)

    # Padroniza nome da coluna de data de execução
    if "DATA EXECUCAO" in df.columns:
        df = df.rename(columns={"DATA EXECUCAO": "data_execucao"})
    elif "Data execucao" in df.columns:
        df = df.rename(columns={"Data execucao": "data_execucao"})

    # Remove colunas desnecessárias
    cols_para_remover = ['REGIONAL', 'TIPO SERVICO', 'FISCAL', 'EMPRESA', 'DATA ENTREGA', 'RETORNO DE']
    df = _drop_cols_safe(df, cols_para_remover)

    # Auditoria
    df = _add_audit_cols(df)

    # Deduplicação
    dedup_keys = ["UC / MD", "data_execucao", "CODIGO", "TOI", "EQUIPE"]
    df = _deduplicate_df(df, dedup_keys)

    # Adiciona colunas REGIONAL e GRUPO
    df = _add_regional_grupo(df, "EQUIPE")

    # Colunas em maiúsculo
    df.columns = df.columns.str.upper()

    # Conteúdo textual em maiúsculo, sem quebrar datas/nums
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(lambda v: v.upper() if isinstance(v, str) else v)

    return df


def transformar_general(mode: str) -> pd.DataFrame:
    """
    Lê todos os arquivos relatorio_prot_geral*.csv em downloads/, normaliza e retorna um único DataFrame.
    """
    known_cols = ["UC / MD", "Status", "Motivo nao baixado"]
    dfs = _read_all_csvs(DOWNLOADS_DIR, "relatorio_prot_geral*.csv", known_columns=known_cols)
    if not dfs:
        raise FileNotFoundError("Nenhum CSV 'relatorio_prot_geral*.csv' encontrado para processar.")

    df = pd.concat(dfs, ignore_index=True)
    df = _normalize_columns(df)
    df = _normalize_date_columns(df)

    # Padroniza nome da coluna de data de execução
    if "DATA EXECUCAO" in df.columns:
        df = df.rename(columns={"DATA EXECUCAO": "data_execucao"})
    elif "Data execucao" in df.columns:
        df = df.rename(columns={"Data execucao": "data_execucao"})

    # Remove colunas desnecessárias
    cols_para_remover = ['Motivo nao baixado', 'Regional', 'Empresa', 'Sit deixada', 'Fiscal', 'tipo_servico_comercial', 'obs_at', 'RS Entrada', 'Lancado por', 'Data lancado', "Hora"]
    df = _drop_cols_safe(df, cols_para_remover)

    # Converte colunas de hora para TIME
    for tcol in ["Hora inicio servico", "Hora fim servico"]:
        if tcol in df.columns:
            df[tcol] = _parse_time_series(df[tcol])

    # Auditoria
    df = _add_audit_cols(df)

    # Deduplicação
    dedup_keys = ["UC / MD", "data_execucao", "Cod", "TOI", "Equipe"]
    df = _deduplicate_df(df, dedup_keys)

    # Adiciona colunas REGIONAL e GRUPO
    df = _add_regional_grupo(df, "Equipe")

    # Colunas em maiúsculo
    df.columns = df.columns.str.upper()

    # Conteúdo textual em maiúsculo, sem quebrar datas/nums
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(lambda v: v.upper() if isinstance(v, str) else v)

    return df