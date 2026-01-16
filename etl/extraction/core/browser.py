"""Módulo para configuração e controle do navegador Selenium."""

import os

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

USUARIO = os.getenv('SIGOS_USUARIO')
SENHA = os.getenv('SIGOS_SENHA')

DOWNLOAD_DIR = os.path.join(os.getcwd(), 'etl', 'downloads')

HEADLESS = os.getenv('HEADLESS', 'true').lower() == 'true'


def esperar_elemento(driver, xpath, tipo='presenca', timeout=20):
    """
    Aguarda a presença, visibilidade ou clicabilidade de um elemento.

    Args:
        driver: Instância do WebDriver do Selenium.
        xpath: XPath do elemento a ser aguardado.
        tipo: Tipo de espera ('presenca', 'visivel' ou 'clicavel').
        timeout: Tempo máximo de espera em segundos.

    Returns:
        WebElement encontrado.

    Raises:
        ValueError: Se o tipo informado for inválido.
    """
    wait = WebDriverWait(driver, timeout)
    if tipo == 'presenca':
        return wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    elif tipo == 'visivel':
        return wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
    elif tipo == 'clicavel':
        elem = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        # Scroll pra garantir que fica visível no headless
        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", elem
            )
        except Exception:
            pass
        return elem
    else:
        raise ValueError(
            "Tipo inválido: use 'presenca', 'visivel' ou 'clicavel'"
        )
    
    
def abre_navegador():
    """
    Configura e abre uma instância do Chrome com opções de download.
    Se SELENIUM_URL estiver definido, usa Remote; caso contrário, usa local.

    Returns:
        WebDriver configurado e apontando para a URL do SIGOS.
    """
    options = Options()

    prefs = {
        'download.default_directory': DOWNLOAD_DIR,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True,
        'safebrowsing.disable_download_protection': True,
    }
    options.add_experimental_option('prefs', prefs)
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--no-first-run')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    if HEADLESS:
        options.add_argument('--headless=new')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-gpu')

    chrome_bin = os.getenv('CHROME_BIN', '')
    if chrome_bin:
        options.binary_location = chrome_bin

    chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
    service = Service(executable_path=chromedriver_path)

    driver = webdriver.Chrome(service=service, options=options)
    driver.get('https://apps.equatorialenergia.com.br/sigos/')
    return driver


def logar_sigos():
    """
    Realiza login no sistema SIGOS usando credenciais do .env.

    Returns:
        WebDriver autenticado e pronto para navegação.
    """
    driver = abre_navegador()
    if not HEADLESS:  # só maximiza se não for headless
        driver.maximize_window()
    campo_login = esperar_elemento(
        driver, '/html/body/form/div/div/div[2]/div[3]/div/div[1]/input'
    )
    campo_login.send_keys(USUARIO)
    campo_senha = esperar_elemento(
        driver, '/html/body/form/div/div/div[2]/div[3]/div/div[2]/input'
    )
    campo_senha.send_keys(SENHA)
    botao_entrar = esperar_elemento(
        driver,
        '/html/body/form/div/div/div[2]/div[3]/div/div[3]/div[2]/button',
        tipo='clicavel',
    )
    botao_entrar.click()
    return driver
