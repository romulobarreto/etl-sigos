"""Módulo para transformação de dados extraídos do sistema SIGOS."""

import csv
import glob
import logging
import os
import unicodedata
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# Bases de caminho
DOWNLOADS_DIR = os.path.join(os.getcwd(), 'etl', 'downloads')

# ====
# Helpers de leitura robusta
# ====


def _sniff_delimiter(path, sample_bytes=8192):
    """
    Detecta o delimitador de um arquivo CSV analisando uma amostra.

    Args:
        path: Caminho do arquivo CSV.
        sample_bytes: Quantidade de bytes a ler para análise (padrão: 8192).

    Returns:
        Delimitador detectado ou None se falhar.
    """
    try:
        with open(path, 'r', encoding='latin1', errors='ignore') as f:
            sample = f.read(sample_bytes)
        dialect = csv.Sniffer().sniff(sample, delimiters=';,|\t')
        return dialect.delimiter
    except Exception:
        return None  # deixa o pandas inferir (sep=None com engine='python')


def _robust_read_csv(path, force_skip=False, known_columns=None):
    """
    Lê um arquivo CSV de forma robusta, tentando múltiplas estratégias.

    Args:
        path: Caminho do arquivo CSV.
        force_skip: Se True, pula linhas malformadas.
        known_columns: Lista de colunas esperadas para realinhamento de header.

    Returns:
        DataFrame lido com sucesso.

    Raises:
        Exception: Se todas as tentativas de leitura falharem.
    """
    attempts = []

    # 1) Detectar separador
    detected = _sniff_delimiter(path)
    if detected:
        attempts.append(dict(sep=detected, engine='python'))
    # 2) Deixar pandas inferir
    attempts.append(dict(sep=None, engine='python'))
    # 3) Forçar semicolon
    attempts.append(dict(sep=';', engine='python'))
    # 4) Forçar comma
    attempts.append(dict(sep=',', engine='python'))
    # 5) Forçar tab
    attempts.append(dict(sep='\t', engine='python'))

    # Se pediu para pular linhas ruins, adiciona variações com on_bad_lines='skip'
    if force_skip:
        attempts = [dict(**a, on_bad_lines='skip') for a in attempts]

    last_exc = None
    for opts in attempts:
        try:
            df = pd.read_csv(
                path,
                encoding='latin1',
                dtype=str,
                keep_default_na=False,
                quoting=csv.QUOTE_MINIMAL,
                **opts,
            )
            # Se o arquivo tiver "lixo" antes do header, tenta realinhar baseado em colunas conhecidas
            if known_columns:
                has_any = any(col in df.columns for col in known_columns)
                if not has_any:
                    with open(
                        path, 'r', encoding='latin1', errors='ignore'
                    ) as f:
                        lines = f.readlines()
                    header_idx = None
                    for idx, line in enumerate(lines[:100]):
                        if any(col in line for col in known_columns):
                            header_idx = idx
                            break
                    if header_idx is not None:
                        df = pd.read_csv(
                            path,
                            encoding='latin1',
                            dtype=str,
                            keep_default_na=False,
                            skiprows=header_idx,
                            **opts,
                        )
            logger.info(
                f'Lido com sucesso: {os.path.basename(path)} usando opts={opts}'
            )
            return df
        except Exception as e:
            last_exc = e
            logger.warning(
                f'Falha lendo {os.path.basename(path)} com opts={opts}: {e}'
            )

    # Última cartada: repetir com skip
    if not force_skip:
        logger.warning(
            f"Tentativas padrão falharam para {os.path.basename(path)}. Repetindo com on_bad_lines='skip'."
        )
        return _robust_read_csv(
            path, force_skip=True, known_columns=known_columns
        )

    # Se nada deu, estoura
    raise last_exc


def _read_all_csvs(folder, pattern, known_columns=None):
    """
    Lê todos os arquivos CSV que correspondem ao pattern na pasta.

    Args:
        folder: Caminho da pasta.
        pattern: Pattern glob para busca de arquivos.
        known_columns: Lista de colunas esperadas para realinhamento.

    Returns:
        Lista de DataFrames lidos.
    """
    paths = sorted(glob.glob(os.path.join(folder, pattern)))
    if not paths:
        logger.warning(
            f'Nenhum arquivo encontrado em {folder} com pattern {pattern}'
        )
        return []
    dfs = []
    for p in paths:
        try:
            df = _robust_read_csv(p, known_columns=known_columns)
            dfs.append(df)
        except Exception as e:
            logger.exception(
                f'Falha definitiva ao ler {os.path.basename(p)}: {e}'
            )
    return dfs


# ====
# Helpers de normalização
# ====


def _norm_col(s: str) -> str:
    """
    Normaliza nome de coluna removendo acentos e espaços extras.

    Args:
        s: String a ser normalizada.

    Returns:
        String normalizada.
    """
    if not isinstance(s, str):
        return s
    s = s.replace('"', '').strip()
    s = ' '.join(s.split())
    s = ''.join(
        c
        for c in unicodedata.normalize('NFKD', s)
        if not unicodedata.combining(c)
    )
    return s


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas do DataFrame.

    Args:
        df: DataFrame a ser normalizado.

    Returns:
        DataFrame com colunas normalizadas.
    """
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    return df


def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas com 'DATA' no nome para datetime.date.

    Faz parse manual de dd/mm/yyyy para yyyy-mm-dd evitando ambiguidade.

    Args:
        df: DataFrame a ser processado.

    Returns:
        DataFrame com colunas de data convertidas.
    """
    df = df.copy()
    for col in df.columns:
        if 'DATA' in col.upper():
            logger.info(f'Convertendo coluna de data: {col}')

            # Limpa valores inválidos
            serie = df[col].astype(str).str.strip()
            invalidos = {
                '',
                'NULL',
                'null',
                'None',
                '0000-00-00',
                '00/00/0000',
                'nan',
            }
            serie = serie.where(~serie.isin(invalidos), None)

            def parse_date_manual(val):
                if val is None or pd.isna(val):
                    return None

                s = str(val).strip()

                # Parse manual: dd/mm/yyyy -> yyyy-mm-dd
                partes = s.split('/')
                if len(partes) == 3:
                    dia, mes, ano = partes
                    iso_str = f'{ano}-{mes.zfill(2)}-{dia.zfill(2)}'
                    ts = pd.to_datetime(iso_str, format='%Y-%m-%d', errors='coerce')
                    return None if pd.isna(ts) else ts.date()

                # Fallback: tenta inferir outros formatos
                ts = pd.to_datetime(s, errors='coerce')
                return None if pd.isna(ts) else ts.date()

            df[col] = serie.apply(parse_date_manual)

    return df


def _parse_time_series(series: pd.Series) -> pd.Series:
    """
    Converte série de strings HH:MM[:SS] para objetos time.

    Args:
        series: Série pandas com valores de hora.

    Returns:
        Série convertida para time.
    """
    return pd.to_datetime(
        series, format='%H:%M:%S', errors='coerce'
    ).dt.time.fillna(
        pd.to_datetime(series, format='%H:%M', errors='coerce').dt.time
    )


def _add_audit_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna de auditoria data_extracao ao DataFrame.

    Args:
        df: DataFrame a ser processado.

    Returns:
        DataFrame com coluna de auditoria.
    """
    df = df.copy()
    df['data_extracao'] = datetime.now()  # timestamp do ETL
    return df


def _drop_cols_safe(df: pd.DataFrame, cols_to_drop: list[str]) -> pd.DataFrame:
    """
    Remove colunas do DataFrame de forma segura (ignora se não existirem).

    Args:
        df: DataFrame a ser processado.
        cols_to_drop: Lista de colunas a remover.

    Returns:
        DataFrame sem as colunas especificadas.
    """
    df = df.copy()
    existing = [c for c in cols_to_drop if c in df.columns]
    if existing:
        df = df.drop(columns=existing)
    return df


def _deduplicate_df(df: pd.DataFrame, subset_keys: list[str]) -> pd.DataFrame:
    """
    Remove duplicatas mantendo a versão mais recente (baseado em data_extracao).

    Args:
        df: DataFrame a ser deduplicado.
        subset_keys: Lista de colunas-chave para identificar duplicatas.

    Returns:
        DataFrame deduplicado.
    """
    # Filtra apenas as chaves que existem no DataFrame
    existing_keys = [k for k in subset_keys if k in df.columns]

    if not existing_keys:
        logger.warning(
            f'Nenhuma chave de deduplicação encontrada: {subset_keys}'
        )
        return df

    initial_count = len(df)

    # Ordena por data_extracao (mais antigo -> mais novo) e remove duplicatas mantendo o último
    df_dedup = df.sort_values('data_extracao').drop_duplicates(
        subset=existing_keys, keep='last'
    )

    final_count = len(df_dedup)
    removed_count = initial_count - final_count

    if removed_count > 0:
        logger.info(
            f'Deduplicação: removidas {removed_count} linhas duplicadas de {initial_count} (chaves: {existing_keys})'
        )
    else:
        logger.info(
            f'Deduplicação: nenhuma duplicata encontrada em {initial_count} linhas'
        )

    return df_dedup


def _add_regional_grupo(df: pd.DataFrame, equipe_col: str) -> pd.DataFrame:
    """
    Adiciona colunas REGIONAL e GRUPO baseadas na coluna de equipe.

    Args:
        df: DataFrame a ser processado.
        equipe_col: Nome da coluna de equipe.

    Returns:
        DataFrame com colunas REGIONAL e GRUPO adicionadas.
    """
    if equipe_col in df.columns:
        df['REGIONAL'] = df[equipe_col].apply(
            lambda x: 'SUL' if isinstance(x, str) and 'PEL' in x else 'NORTE'
        )
        df['GRUPO'] = df[equipe_col].apply(
            lambda x: 'AT' if isinstance(x, str) and 'A0' in x else 'BT'
        )
    return df


# ====
# Transformadores principais
# ====


def transformar_return(mode: str) -> pd.DataFrame:
    """
    Lê e transforma todos os arquivos de retorno em um único DataFrame.

    Args:
        mode: Modo de execução ('full' ou 'incremental').

    Returns:
        DataFrame consolidado e transformado.

    Raises:
        FileNotFoundError: Se nenhum arquivo de retorno for encontrado.
    """
    known_cols = [
        'REGIONAL',
        'UC / MD',
        'TIPO SERVICO',
        'DATA EXECUCAO',
        'CODIGO',
    ]
    dfs = _read_all_csvs(
        DOWNLOADS_DIR, 'retorno*.csv', known_columns=known_cols
    )
    if not dfs:
        raise FileNotFoundError(
            'Nenhum CSV de retorno encontrado para processar.'
        )

    df = pd.concat(dfs, ignore_index=True)
    df = _normalize_columns(df)
    df = _normalize_date_columns(df)

    # Padroniza nome da coluna de data de execução
    if 'DATA EXECUCAO' in df.columns:
        df = df.rename(columns={'DATA EXECUCAO': 'data_execucao'})
    elif 'Data execucao' in df.columns:
        df = df.rename(columns={'Data execucao': 'data_execucao'})

    # Remove colunas desnecessárias
    cols_para_remover = [
        'REGIONAL',
        'TIPO SERVICO',
        'FISCAL',
        'EMPRESA',
        'DATA ENTREGA',
        'RETORNO DE',
    ]
    df = _drop_cols_safe(df, cols_para_remover)

    # Auditoria
    df = _add_audit_cols(df)

    # Deduplicação
    dedup_keys = ['UC / MD', 'data_execucao', 'CODIGO', 'TOI', 'EQUIPE']
    df = _deduplicate_df(df, dedup_keys)

    # Adiciona colunas REGIONAL e GRUPO
    df = _add_regional_grupo(df, 'EQUIPE')

    # Colunas em maiúsculo
    df.columns = df.columns.str.upper()

    # Conteúdo textual em maiúsculo, sem quebrar datas/nums
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(
                lambda v: v.upper() if isinstance(v, str) else v
            )

    return df


def transformar_general(mode: str) -> pd.DataFrame:
    """
    Lê e transforma todos os arquivos de relatório geral em um único DataFrame.

    Args:
        mode: Modo de execução ('full' ou 'incremental').

    Returns:
        DataFrame consolidado e transformado.

    Raises:
        FileNotFoundError: Se nenhum arquivo de relatório geral for encontrado.
    """
    known_cols = ['UC / MD', 'Status', 'Motivo nao baixado']
    dfs = _read_all_csvs(
        DOWNLOADS_DIR, 'relatorio_prot_geral*.csv', known_columns=known_cols
    )
    if not dfs:
        raise FileNotFoundError(
            "Nenhum CSV 'relatorio_prot_geral*.csv' encontrado para processar."
        )

    df = pd.concat(dfs, ignore_index=True)
    df = _normalize_columns(df)
    df = _normalize_date_columns(df)

    # Padroniza nome da coluna de data de execução
    if 'DATA EXECUCAO' in df.columns:
        df = df.rename(columns={'DATA EXECUCAO': 'data_execucao'})
    elif 'Data execucao' in df.columns:
        df = df.rename(columns={'Data execucao': 'data_execucao'})

    # Remove colunas desnecessárias
    cols_para_remover = [
        'Motivo nao baixado',
        'Regional',
        'Empresa',
        'Sit deixada',
        'Fiscal',
        'tipo_servico_comercial',
        'obs_at',
        'RS Entrada',
        'Lancado por',
        'Data lancado',
        'Hora',
    ]
    df = _drop_cols_safe(df, cols_para_remover)

    # Converte colunas de hora para TIME
    for tcol in ['Hora inicio servico', 'Hora fim servico']:
        if tcol in df.columns:
            df[tcol] = _parse_time_series(df[tcol])

    # Auditoria
    df = _add_audit_cols(df)

    # Deduplicação
    dedup_keys = ['UC / MD', 'data_execucao', 'Cod', 'TOI', 'Equipe']
    df = _deduplicate_df(df, dedup_keys)

    # Adiciona colunas REGIONAL e GRUPO
    df = _add_regional_grupo(df, 'Equipe')

    # Colunas em maiúsculo
    df.columns = df.columns.str.upper()

    # Conteúdo textual em maiúsculo, sem quebrar datas/nums
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(
                lambda v: v.upper() if isinstance(v, str) else v
            )

    return df
