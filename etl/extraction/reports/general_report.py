from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from extraction.core.browser import esperar_elemento, logar_sigos
from extraction.core.utils import esperar_download_concluir, gerar_intervalos
from datetime import datetime, timedelta, date
import time
import os

DOWNLOAD_DIR = os.path.join(os.getcwd(), "etl", "downloads")

def digitar_data_por_etapas(actions, data_str):
    """Recebe 'dd/mm/yyyy' e digita em 3 etapas: dia, mês, ano"""
    dia, mes, ano = data_str.split("/")
    # digita devagar com pausa entre cada parte
    actions.send_keys(dia).pause(0.5)
    actions.send_keys(mes).pause(0.5)
    actions.send_keys(ano).pause(0.5)
    return actions


def exportar_geral(driver, data_inicio, data_final, primeira_vez=False):
    actions = ActionChains(driver)

    if primeira_vez:
        # Abre o menu de relatórios
        botao_relatorios = esperar_elemento(driver, "/html/body/div[1]/aside[1]/div/nav/ul/li[7]/a/i", tipo="clicavel")
        botao_relatorios.click()

        botao_relatorios2 = esperar_elemento(driver, "/html/body/div[1]/aside[1]/div/nav/ul/li[7]/ul/li[1]/a/i", tipo="clicavel")
        botao_relatorios2.click()

        # Seleciona "Tudo"
        tipo_relatorio_elem = esperar_elemento(driver, '//*[@id="tp_relatorio"]', tipo="clicavel")
        select = Select(tipo_relatorio_elem)
        select.select_by_value("tudo")

        # Seleciona "Por data do serviço"
        tipo_periodo_elem = esperar_elemento(driver, '//*[@id="periodo_todos"]', tipo="clicavel")
        select = Select(tipo_periodo_elem)
        select.select_by_value("data_execucao")

        # Preenche as datas
        # Data inicial
        data_inicio_elem = esperar_elemento(driver, '//*[@id="data_inicio"]', tipo="clicavel")
        data_inicio_elem.clear()
        data_inicio_elem.click()
        actions = digitar_data_por_etapas(actions, data_inicio)
        actions.perform()
        # Data final
        data_final_elem = esperar_elemento(driver, '//*[@id="data_fim"]', tipo="clicavel")
        data_final_elem.clear()
        data_final_elem.click()
        actions = digitar_data_por_etapas(actions, data_final)
        actions.perform()
        # Clica no botão de exportar
        botao_exportar_elem = esperar_elemento(driver, '//*[@id="btn-salvar-form"]', tipo="clicavel")
        botao_exportar_elem.click()

    else:
        # Preenche as datas
        # Data inicial
        data_inicio_elem = esperar_elemento(driver, '//*[@id="data_inicio"]', tipo="clicavel")
        data_inicio_elem.clear()
        data_inicio_elem.click()
        actions = digitar_data_por_etapas(actions, data_inicio)
        actions.perform()
        # Data final
        data_final_elem = esperar_elemento(driver, '//*[@id="data_fim"]', tipo="clicavel")
        data_final_elem.clear()
        data_final_elem.click()
        actions = digitar_data_por_etapas(actions, data_final)
        actions.perform()
        # Clica no botão de exportar
        botao_exportar_elem = esperar_elemento(driver, '//*[@id="btn-salvar-form"]', tipo="clicavel")
        botao_exportar_elem.click()

    print(f"Exportando relatório: {data_inicio} até {data_final}")

def download_general_report(mode="full"):
    driver = logar_sigos()
    hoje = date.today()

    if mode == "full":
        data_inicio_coleta = datetime.strptime("01/03/2022", "%d/%m/%Y")
        data_fim_coleta = datetime.combine(hoje, datetime.min.time())
    elif mode == "incremental":
        data_inicio_coleta = datetime.combine(hoje - timedelta(days=60), datetime.min.time())
        data_fim_coleta = datetime.combine(hoje, datetime.min.time())
    else:
        raise ValueError("Modo de execução inválido. Use 'full' ou 'incremental'.")

    for i, (data_inicio, data_final) in enumerate(
        gerar_intervalos(
            data_inicio_coleta.strftime("%d/%m/%Y"),
            data_fim_coleta.strftime("%d/%m/%Y")
        )
    ):
        primeira_vez = i == 0
        exportar_geral(driver, data_inicio, data_final, primeira_vez=primeira_vez)
        esperar_download_concluir(pasta=DOWNLOAD_DIR)
        print(f"Download concluído: {data_inicio} a {data_final}")
        time.sleep(2)

    driver.quit()