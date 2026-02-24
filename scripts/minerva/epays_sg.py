import os
import time
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# set up logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
username = os.getenv("minerva_username")
password = os.getenv("minerva_password")

if not username or not password:
    raise ValueError("Environment variables 'user' and/or 'password' not set.")

download_dir = os.getcwd()  

# set up chrome options for headless mode/configure download behavior
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--enable-downloads")  # Explicitly enable downloads
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")  # Set dimensions
chrome_options.add_argument("--start-maximized")  # Maximize window
chrome_options.add_argument("--force-device-scale-factor=1")  # Prevent scaling

prefs = {
    "download.default_directory": download_dir,  # set download path
    "plugins.always_open_pdf_externally": True, # auto-downloads pdf files instead of opening in new window
    "download.open_pdf_in_system_reader": False,
    "pdfjs.disabled": True,  # Disable built-in PDF viewer
    "download.prompt_for_download": False,  # disable prompt
    "directory_upgrade": True,  # auto-overwrite existing files
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)

# initialize webdriver
driver = webdriver.Chrome(options=chrome_options)

# start download process 
try:
    logging.info("Navigate to the target URL and login")
    driver.get("https://meuclube.epays.com.br/login")

    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="cpf"]'))).send_keys(username)
    print('cpf inserido')
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="login-form"]/div[2]/div/input'))).send_keys(password)
    print('senha inserida')
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="login-form"]/div[4]/action-button/button/span[1]'))).click()
    print('acessar clicado')

    empresa_nome = 'São Gabriel'
    tentativas = 5

    for tentativa in range(tentativas):
        try:
            time.sleep(5)
            campo_empresa = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="empresa"]')))
            campo_empresa.clear()
            campo_empresa.send_keys(empresa_nome)
            print(f'{empresa_nome} escrito (tentativa {tentativa+1})')
            time.sleep(5)

            sugestao = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="ngb-typeahead-0-0"]/ngb-highlight/span'))
            )
            sugestao.click()
            print(f'{empresa_nome} selecionado')
            break  # Sai do loop se der certo
        except Exception as e:
                print(f'Falha ao selecionar empresa ({tentativa+1}/5): {e}')
                if tentativa == tentativas - 1:
                    raise Exception(f'Não foi possível selecionar a empresa "{empresa_nome}" após {tentativas} tentativas.')

    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="perfil"]'))).click()
    print('abrindo lista parceiro')
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="perfil"]/option[2]'))).click()
    print('selecionando PARCEIRO')

    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="select-company-form"]/div[4]/action-button/button/span[1]'))).click()
    print('acessar clicado')

    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/app-partners/internal-view/div/navigation-bar/nav/div[1]/span/button'))).click()
    print('menu lateral clicado')
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/app-partners/internal-view/div/navigation-bar/div/div/navigation-menu/div/navigation-item[3]/div/div[2]/div'))).click()
    print('periodo clicado')

    # Verifica o dia atual
    dia_hoje = datetime.now().day

    # Define o XPath com base no dia
    if dia_hoje == 16:
        xpath_download = '/html/body/app-root/app-partners/internal-view/div/main/div/div/time-course/div/div/time-course-card[2]/div/div[2]/div/action-button/button/span[1]/i'
        print('Hoje é dia 16 — baixando penúltimo período')
    else:
        xpath_download = '/html/body/app-root/app-partners/internal-view/div/main/div/div/time-course/div/div/time-course-card[1]/div/div[2]/div/action-button/button/span[1]/i'
        print('Baixando último período normalmente')

    # Realiza o clique no botão de download apropriado
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath_download))).click()
    logging.info('download clicado')

    time.sleep(10)  # Esperar o download completar
    logging.info('download completo')

    # get the most recent downloaded file
    files = os.listdir(download_dir)
    downloaded_files = [f for f in files if f.endswith('.xlsx')]
    if downloaded_files:
        # sort files by modifi time
        downloaded_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)))
        most_recent_file = downloaded_files[-1]  # get the most recent file
        downloaded_file_path = os.path.join(download_dir, most_recent_file)

        # log the final file path and size
        file_size = os.path.getsize(downloaded_file_path)
        logging.info(f"Download completed successfully. File path: {downloaded_file_path}, Size: {file_size} bytes")
    else:
        logging.error("Download failed. No files found.")

finally:
    driver.quit()
