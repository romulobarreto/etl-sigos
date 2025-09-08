from fastapi import FastAPI
import subprocess
import psycopg2
import os
from psycopg2.extras import RealDictCursor

app = FastAPI()

# ==============================
# Configuração do Banco (via ENV)
# ==============================
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_NAME = os.getenv("DB_NAME", "etl_db")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )

# ==============================
# Healthcheck
# ==============================
@app.get("/")
def read_root():
    return {"message": "API ETL Online 🚀"}

# ==============================
# Disparar o ETL já existente
# ==============================
@app.post("/etl/run")
def run_etl(report: str, mode: str):
    """
    Dispara o ETL existente chamando main.py.
    Exemplo:
    POST /etl/run?report=general&mode=incremental
    """
    try:
        result = subprocess.run(
            ["python", "main.py", "--report", report, "--mode", mode],
            capture_output=True, text=True, cwd="/app/etl"
        )
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==============================
# Dados para Power BI
# ==============================
@app.get("/data/general")
def get_general():
    """
    Retorna todos os registros da tabela general_reports em formato JSON.
    """
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM general_reports ORDER BY data_extracao DESC;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        return {"error": str(e)}

@app.get("/data/return")
def get_return():
    """
    Retorna todos os registros da tabela return_reports em formato JSON.
    """
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM return_reports ORDER BY data_extracao DESC;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        return {"error": str(e)}