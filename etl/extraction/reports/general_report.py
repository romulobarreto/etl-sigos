"""Módulo para extração do relatório geral do sistema SIGOS."""

import os
import time
from datetime import date, datetime, timedelta

from extraction.core.browser import esperar_elemento, logar_sigos
from extraction.core.utils import esperar_download_concluir, gerar_intervalos
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

DOWNLOAD_DIR = os.path.join(os.getcwd(), 'etl', 'downloads')


def digitar_data_por_etapas(actions, data_str):
    """
    Digita uma data em etapas (dia, mês, ano) com pausas.

    Args:
        actions: Instância de ActionChains do Selenium.
        data_str: Data no formato 'dd/mm/yyyy'.

    Returns:
        ActionChains atualizado com as ações de digitação.
    """
    dia, mes, ano = data_str.split('/')
    # digita devagar com pausa entre cada parte
    actions.send_keys(mes).pause(0.5)
    actions.send_keys(dia).pause(0.5)
    actions.send_keys(ano).pause(0.5)
    return actions


def exportar_geral(driver, data_inicio, data_final, primeira_vez=False):
    """
    Exporta o relatório geral do SIGOS para um intervalo de datas.

    Args:
        driver: Instância do WebDriver do Selenium.
        data_inicio: Data inicial no formato 'dd/mm/yyyy'.
        data_final: Data final no formato 'dd/mm/yyyy'.
        primeira_vez: Se True, navega até o menu de relatórios.
    """
    actions = ActionChains(driver)

    if primeira_vez:
        # Abre o menu de relatórios
        botao_relatorios = esperar_elemento(
            driver,
            '/html/body/div[1]/aside[1]/div/nav/ul/li[7]/a/i',
            tipo='clicavel',
        )
        botao_relatorios.click()

        botao_relatorios2 = esperar_elemento(
            driver,
            '/html/body/div[1]/aside[1]/div/nav/ul/li[7]/ul/li[1]/a/i',
            tipo='clicavel',
        )
        botao_relatorios2.click()

        # Seleciona "Tudo"
        tipo_relatorio_elem = esperar_elemento(
            driver, '//*[@id="tp_relatorio"]', tipo='clicavel'
        )
        select = Select(tipo_relatorio_elem)
        select.select_by_value('tudo')

        # Seleciona "Por data do serviço"
        tipo_periodo_elem = esperar_elemento(
            driver, '//*[@id="periodo_todos"]', tipo='clicavel'
        )
        select = Select(tipo_periodo_elem)
        select.select_by_value('data_execucao')

        # Preenche as datas
        # Data inicial
        data_inicio_elem = esperar_elemento(
            driver, '//*[@id="data_inicio"]', tipo='clicavel'
        )
        data_inicio_elem.clear()
        data_inicio_elem.click()
        actions = digitar_data_por_etapas(actions, data_inicio)
        actions.perform()
        # Data final
        data_final_elem = esperar_elemento(
            driver, '//*[@id="data_fim"]', tipo='clicavel'
        )
        data_final_elem.clear()
        data_final_elem.click()
        actions = digitar_data_por_etapas(actions, data_final)
        actions.perform()
        # Clica no botão de exportar
        botao_exportar_elem = esperar_elemento(
            driver, '//*[@id="btn-salvar-form"]', tipo='clicavel'
        )
        botao_exportar_elem.click()

    else:
        # Preenche as datas
        # Data inicial
        data_inicio_elem = esperar_elemento(
            driver, '//*[@id="data_inicio"]', tipo='clicavel'
        )
        data_inicio_elem.clear()
        data_inicio_elem.click()
        actions = digitar_data_por_etapas(actions, data_inicio)
        actions.perform()
        # Data final
        data_final_elem = esperar_elemento(
            driver, '//*[@id="data_fim"]', tipo='clicavel'
        )
        data_final_elem.clear()
        data_final_elem.click()
        actions = digitar_data_por_etapas(actions, data_final)
        actions.perform()
        # Clica no botão de exportar
        botao_exportar_elem = esperar_elemento(
            driver, '//*[@id="btn-salvar-form"]', tipo='clicavel'
        )
        botao_exportar_elem.click()

    print(f'Exportando relatório: {data_inicio} até {data_final}')


def download_general_report(mode='full'):
    """
    Realiza o download do relatório geral do SIGOS.

    Args:
        mode: Modo de execução ('full' ou 'incremental').

    Raises:
        ValueError: Se o modo informado for inválido.
    """
    driver = logar_sigos()
    hoje = date.today()

    if mode == 'full':
        data_inicio_coleta = datetime.strptime('01/03/2022', '%d/%m/%Y')
        data_fim_coleta = datetime.combine(hoje, datetime.min.time())
    elif mode == 'incremental':
        data_inicio_coleta = datetime.combine(
            hoje - timedelta(days=60), datetime.min.time()
        )
        data_fim_coleta = datetime.combine(hoje, datetime.min.time())
    else:
        raise ValueError(
            "Modo de execução inválido. Use 'full' ou 'incremental'."
        )

    for i, (data_inicio, data_final) in enumerate(
        gerar_intervalos(
            data_inicio_coleta.strftime('%d/%m/%Y'),
            data_fim_coleta.strftime('%d/%m/%Y'),
        )
    ):
        primeira_vez = i == 0
        exportar_geral(
            driver, data_inicio, data_final, primeira_vez=primeira_vez
        )
        esperar_download_concluir(pasta=DOWNLOAD_DIR)
        print(f'Download concluído: {data_inicio} a {data_final}')
        time.sleep(2)

    driver.quit()
