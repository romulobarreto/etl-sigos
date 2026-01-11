# tests/test_transformer.py
import pytest
import pandas as pd
from datetime import date
from etl.transformation.transformer import _norm_col, _normalize_date_columns

def test_norm_col_limpeza_strings():
    """Garante que a normalização de colunas remove acentos e espaços extras."""
    assert _norm_col("  Coração de Maçã  ") == "Coracao de Maca"
    assert _norm_col("DATA   EXECUÇÃO") == "DATA EXECUCAO"
    assert _norm_col(None) is None

def test_normalize_date_columns_sucesso():
    """Valida se a conversão manual de dd/mm/yyyy para date funciona corretamente."""
    df = pd.DataFrame({
        'DATA_EXECUCAO': ['01/11/2025', '15/03/2024'],
        'OUTRA_COLUNA': ['TESTE', 'TESTE']
    })
    
    df_clean = _normalize_date_columns(df)
    
    # Verifica se virou objeto date e se a ordem está correta (YYYY-MM-DD)
    assert df_clean['DATA_EXECUCAO'].iloc[0] == date(2025, 11, 1)
    assert df_clean['DATA_EXECUCAO'].iloc[1] == date(2024, 3, 15)

def test_normalize_date_columns_invalidas():
    """Garante que datas '00/00/0000' ou vazias virem None (NULL no banco)."""
    df = pd.DataFrame({
        'DATA_BAIXADO': ['00/00/0000', '', 'NULL', 'nan']
    })
    
    df_clean = _normalize_date_columns(df)
    
    assert df_clean['DATA_BAIXADO'].iloc[0] is None
    assert df_clean['DATA_BAIXADO'].iloc[1] is None
    assert df_clean['DATA_BAIXADO'].iloc[2] is None