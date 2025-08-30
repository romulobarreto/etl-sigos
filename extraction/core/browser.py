from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
from dotenv import load_dotenv

load_dotenv()

USUARIO = os.getenv("SIGOS_USUARIO")
SENHA = os.getenv("SIGOS_SENHA")
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "downloads")
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"  # true por padrão


def esperar_elemento(driver, xpath, tipo="presenca", timeout=20):
    wait = WebDriverWait(driver, timeout)
    if tipo == "presenca":
        return wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    elif tipo == "visivel":
        return wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
    elif tipo == "clicavel":
        return wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    else:
        raise ValueError("Tipo inválido: use 'presenca', 'visivel' ou 'clicavel'")


def abre_navegador():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIR,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    })

    # Silencia os logs extras do Chrome
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get("https://apps.equatorialenergia.com.br/sigos/")
    return driver


def logar_sigos():
    driver = abre_navegador()
    campo_login = esperar_elemento(driver, "/html/body/form/div/div/div[2]/div[3]/div/div[1]/input")
    campo_login.send_keys(USUARIO)
    campo_senha = esperar_elemento(driver, "/html/body/form/div/div/div[2]/div[3]/div/div[2]/input")
    campo_senha.send_keys(SENHA)
    botao_entrar = esperar_elemento(driver, "/html/body/form/div/div/div[2]/div[3]/div/div[3]/div[2]/button", tipo="clicavel")
    botao_entrar.click()
    return driver