# tests/test_loader.py
import pytest 
from etl.load.loader import get_engine
from sqlalchemy import text

def test_db_connection():
    """Verifica se a conexão com o PostgreSQL está ativa."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            # scalar() é mais limpo que fetchone()[0]
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    except Exception as e:
        # Agora o pytest.fail vai funcionar porque o import está lá em cima
        pytest.fail(f"Falha ao conectar no banco de dados: {e}")