from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

USUARIO = os.getenv("SIGOS_USUARIO")
SENHA = os.getenv("SIGOS_SENHA")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "etl", "downloads")

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"  


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

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    # Flags extras
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")

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
    driver.maximize_window()
    campo_login = esperar_elemento(driver, "/html/body/form/div/div/div[2]/div[3]/div/div[1]/input")
    campo_login.send_keys(USUARIO)
    campo_senha = esperar_elemento(driver, "/html/body/form/div/div/div[2]/div[3]/div/div[2]/input")
    campo_senha.send_keys(SENHA)
    botao_entrar = esperar_elemento(driver, "/html/body/form/div/div/div[2]/div[3]/div/div[3]/div[2]/button", tipo="clicavel")
    botao_entrar.click()
    return driver