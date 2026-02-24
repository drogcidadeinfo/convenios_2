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
username = os.getenv("username")
password = os.getenv("password")

if not username or not password:
    raise ValueError("Environment variables 'user' and/or 'password' not set.")

download_dir = os.getcwd()  

# set up chrome options for headless mode/configure download behavior
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--start-maximized")

prefs = {
    "download.default_directory": download_dir,  # set download path
    "download.prompt_for_download": False,  # disable prompt
    "directory_upgrade": True,  # auto-overwrite existing files
    "safebrowsing.enabled": False,  # disable safe browsing (meh)
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")

# initialize webdriver
driver = webdriver.Chrome(options=chrome_options)

# start download process 
try:
    logging.info("Navigate to the target URL and login")
    driver.get("http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="id_cod_usuario"]'))).send_keys(username)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="nom_senha"]'))).send_keys(password)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="login"]'))).click()
    time.sleep(2)

    # button SNGPC
    time.sleep(2.5)
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.F11)
    time.sleep(2.5)
    print("Pop-up fechado com sucesso.")

    # access "Compras Fornecedores"
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sideMenuSearch")))
    driver.find_element(By.ID, "sideMenuSearch").send_keys("Contas Receber ou Recebidas")
    driver.find_element(By.ID, "sideMenuSearch").click()
    time.sleep(5)

    driver.find_element(By.CSS_SELECTOR, '[title="Contas Receber ou Recebidas"]').click()
    time.sleep(5)
  
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="agrup_fil_2"]'))).click()
    print('selecao: agrupar por filial')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sel_contas_2"]'))).click()
    print('selecao: convenio')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="cod_filvendEntrada"]'))).send_keys('14', Keys.ENTER)
    WebDriverWait(driver, 10).until(EC.invisibility_of_element((By.XPATH, '//*[@id="divLoading"]')))
    time.sleep(2)
    print('filial 14 inserida')
    
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="consid_filvend_1"]'))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tabTabdhtmlgoodies_tabView1_1"]/a'))).click()
    print('dados II')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="cod_empresaEntrada"]'))).send_keys('132', Keys.ENTER)
    print('empresa convenio: 132 - FORTUNCERES S.A')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tabTabdhtmlgoodies_tabView1_2"]/a'))).click()
    print('dados III')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="selecao_periodo_1"]'))).click()
    print('periodo por emissao')
    time.sleep(2)

    hoje = datetime.now()
    if hoje.day <= 16:
        inicio = (hoje.replace(day=1) - timedelta(days=1)).replace(day=16)
        fim = hoje.replace(day=15)
    else:
        proximo_mes = (hoje.replace(day=28) + timedelta(days=4)).replace(day=1)
        inicio = hoje.replace(day=16)
        fim = proximo_mes.replace(day=15)

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dat_init"]'))).send_keys(inicio.strftime('%d/%m/%Y'))
    print(f'data inicial: {inicio}')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dat_fim"]'))).send_keys(fim.strftime('%d/%m/%Y'))
    print(f'data final: {fim}')
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="saida_4"]'))).click()
    print('saida XLS')
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="runReport"]'))).click()
    print('gerando relatorio')
    WebDriverWait(driver, 10).until(EC.invisibility_of_element((By.XPATH, '//*[@id="divLoading"]')))
    print('relatorio gerado')
    time.sleep(5)  # esperar download

    # get the most recent downloaded file
    files = os.listdir(download_dir)
    downloaded_files = [f for f in files if f.endswith('.xls')]
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
    time.sleep(5)
    driver.quit()
