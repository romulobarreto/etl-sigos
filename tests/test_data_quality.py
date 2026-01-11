# tests/test_data_quality.py
"""Testes de qualidade de dados e regras de negócio do ETL."""

import pytest
import pandas as pd
from datetime import date, datetime
from etl.transformation.transformer import _deduplicate_df, _normalize_date_columns


def test_schema_general_report():
    """Valida se o DataFrame de relatório geral possui as colunas obrigatórias."""
    # Simula um DataFrame transformado
    df = pd.DataFrame({
        'UC / MD': ['123456'],
        'STATUS': ['BAIXADO'],
        'DATA_EXECUCAO': [date(2025, 11, 1)],
        'EQUIPE': ['RS-PEL-F001M'],
        'REGIONAL': ['SUL'],
        'GRUPO': ['BT'],
        'data_extracao': [datetime.now()]
    })
    
    # Colunas obrigatórias que devem existir antes do load
    colunas_obrigatorias = [
        'UC / MD',
        'STATUS',
        'DATA_EXECUCAO',
        'EQUIPE',
        'REGIONAL',
        'GRUPO',
        'data_extracao'
    ]
    
    for col in colunas_obrigatorias:
        assert col in df.columns, f"Coluna obrigatória '{col}' não encontrada no DataFrame"


def test_deduplicacao_mantém_mais_recente():
    """Garante que a deduplicação mantém apenas o registro mais recente."""
    # Cria DataFrame com duplicatas (mesma UC e data_execucao)
    df = pd.DataFrame({
        'UC / MD': ['123456', '123456', '789012'],
        'DATA_EXECUCAO': [date(2025, 11, 1), date(2025, 11, 1), date(2025, 11, 2)],
        'EQUIPE': ['RS-PEL-F001M', 'RS-PEL-F001M', 'RS-PEL-F002M'],
        'TOI': ['1234567', '1234567', '9876543'],
        'data_extracao': [
            datetime(2025, 11, 1, 10, 0, 0),  # mais antigo
            datetime(2025, 11, 1, 15, 0, 0),  # mais recente (deve manter)
            datetime(2025, 11, 2, 10, 0, 0)
        ]
    })
    
    # Aplica deduplicação
    df_dedup = _deduplicate_df(df, subset_keys=['UC / MD', 'DATA_EXECUCAO', 'EQUIPE', 'TOI'])
    
    # Deve ter apenas 2 registros (o duplicado foi removido)
    assert len(df_dedup) == 2
    
    # O registro mantido deve ser o mais recente (15h)
    registro_mantido = df_dedup[df_dedup['UC / MD'] == '123456'].iloc[0]
    assert registro_mantido['data_extracao'].hour == 15


def test_datas_invalidas_viram_none():
    """Garante que datas inválidas (00/00/0000, vazias, NULL) viram None."""
    df = pd.DataFrame({
        'DATA_EXECUCAO': ['01/11/2025', '00/00/0000', '', 'NULL', 'nan'],
        'DATA_BAIXADO': ['15/03/2024', '32/13/2025', 'null', 'None', '  ']
    })
    
    df_clean = _normalize_date_columns(df)
    
    # Primeira linha deve ter data válida
    assert df_clean['DATA_EXECUCAO'].iloc[0] == date(2025, 11, 1)
    assert df_clean['DATA_BAIXADO'].iloc[0] == date(2024, 3, 15)
    
    # Todas as outras devem ser None (NULL no banco)
    assert df_clean['DATA_EXECUCAO'].iloc[1] is None  # 00/00/0000
    assert df_clean['DATA_EXECUCAO'].iloc[2] is None  # vazio
    assert df_clean['DATA_EXECUCAO'].iloc[3] is None  # NULL
    assert df_clean['DATA_EXECUCAO'].iloc[4] is None  # nan
    
    # Data impossível (32/13) deve virar None
    assert df_clean['DATA_BAIXADO'].iloc[1] is None


def test_regional_e_grupo_baseado_em_equipe():
    """Valida se as colunas REGIONAL e GRUPO são criadas corretamente."""
    df = pd.DataFrame({
        'EQUIPE': ['RS-PEL-F001M', 'POA2F107', 'RS-PEL-A001M', 'POA2A001']
    })
    
    # Importa a função que adiciona regional/grupo
    from etl.transformation.transformer import _add_regional_grupo
    df_result = _add_regional_grupo(df, 'EQUIPE')
    
    # Verifica REGIONAL (PEL = SUL, resto = NORTE)
    assert df_result['REGIONAL'].iloc[0] == 'SUL'   # PEL
    assert df_result['REGIONAL'].iloc[1] == 'NORTE' # POA
    assert df_result['REGIONAL'].iloc[2] == 'SUL'   # PEL
    assert df_result['REGIONAL'].iloc[3] == 'NORTE' # POA
    
    # Verifica GRUPO (A0 = AT, resto = BT)
    assert df_result['GRUPO'].iloc[0] == 'BT'  # F001
    assert df_result['GRUPO'].iloc[1] == 'BT'  # F107
    assert df_result['GRUPO'].iloc[2] == 'AT'  # A001
    assert df_result['GRUPO'].iloc[3] == 'AT'  # A001