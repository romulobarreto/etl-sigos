from datetime import datetime, timedelta
import time
import os

def esperar_download_concluir(pasta, timeout=300):
    # Garante que a pasta existe (cria se não existir)
    os.makedirs(pasta, exist_ok=True)
    
    inicio = time.time()
    arquivos_iniciais = set(os.listdir(pasta))
    
    while True:
        arquivos_temporarios = [f for f in os.listdir(pasta) if f.endswith(".crdownload") or f.endswith(".tmp")]
        
        arquivos_atuais = set(os.listdir(pasta))
        novos_arquivos = arquivos_atuais - arquivos_iniciais
        arquivos_csv_baixados = [f for f in novos_arquivos if f.endswith(".csv")]

        if not arquivos_temporarios and arquivos_csv_baixados:
            print(f"Download(s) concluído(s): {arquivos_csv_baixados}")
            break

        if time.time() - inicio > timeout:
            raise TimeoutError("Download demorou demais e não foi concluído.")
        time.sleep(1)


def gerar_intervalos(data_inicio_str, data_fim_str, dias_por_intervalo=30):
    data_inicio = datetime.strptime(data_inicio_str, "%d/%m/%Y")
    data_fim = datetime.strptime(data_fim_str, "%d/%m/%Y")

    while data_inicio.date() < data_fim.date():
        data_final_intervalo = data_inicio + timedelta(days=dias_por_intervalo)
        if data_final_intervalo.date() > data_fim.date():
            data_final_intervalo = datetime.combine(data_fim.date(), datetime.min.time())
        yield (data_inicio.strftime("%d/%m/%Y"), data_final_intervalo.strftime("%d/%m/%Y"))
        data_inicio = data_final_intervalo