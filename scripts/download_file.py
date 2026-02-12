import os
import time
import logging
import shutil
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# set up logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
username = os.getenv("username")
password = os.getenv("password")

if not username or not password:
    raise ValueError("Environment variables 'user' and/or 'password' not set.")

# Calculate date range
today = datetime.today()
fim_date = today - timedelta(days=1)
inicio_date = datetime(today.year - 1, 1, 1)
inicio = inicio_date.strftime('%d/%m/%Y')
fim = fim_date.strftime('%d/%m/%Y')

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
chrome_options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")

# initialize webdriver
driver = webdriver.Chrome(options=chrome_options)

# start download process 
try:
    logging.info("Navigate to the target URL and login")
    driver.get("http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")
    
    # Add this at startup
    logging.info(f"Download directory set to: {download_dir}")
    os.makedirs(download_dir, exist_ok=True)

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "id_cod_usuario"))).send_keys(username)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "nom_senha"))).send_keys(password)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "login"))).click()

    # wait til page loads completely
    WebDriverWait(driver, 10).until(lambda x: x.execute_script("return document.readyState === 'complete'"))
    time.sleep(15)

    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.F11)

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sideMenuSearch")))
    driver.find_element(By.ID, "sideMenuSearch").send_keys("Contas Receber ou Recebidas")
    driver.find_element(By.ID, "sideMenuSearch").click()
    time.sleep(5)

    driver.find_element(By.CSS_SELECTOR, '[title="Contas Receber ou Recebidas"]').click()
    time.sleep(10)
  
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "sel_contas_2"))).click()
    time.sleep(2)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "tabTabdhtmlgoodies_tabView1_2"))).click()
    time.sleep(2)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "sel2_3"))).click()
    time.sleep(2)

    driver.find_element(By.ID, "dat_init").send_keys(inicio)
    time.sleep(5)
    driver.find_element(By.ID, "dat_fim").send_keys(fim)
    time.sleep(2)
        
    # report format; downloads pdf file
    driver.find_element(By.ID, "saida_4").click()
    time.sleep(2)

    # trigger report download
    logging.info("Triggering report download...")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "runReport"))).click()

    # log download start
    logging.info("Download has started.")
    time.sleep(10)

    # get the most recent downloaded file
    files = os.listdir(download_dir)
    downloaded_files = [f for f in files if f.endswith('.xls')]
    
    if downloaded_files:
        downloaded_files.sort(
            key=lambda x: os.path.getmtime(os.path.join(download_dir, x))
        )
    
        most_recent_file = downloaded_files[-1]
        downloaded_file_path = os.path.join(download_dir, most_recent_file)
    
        # rename the file
        new_filename = "convenio.xls"
        new_filepath = os.path.join(download_dir, new_filename)
    
        os.rename(downloaded_file_path, new_filepath)
    
        file_size = os.path.getsize(new_filepath)
        logging.info(f"File renamed to {new_filename}. Size: {file_size} bytes")
    
    else:
        logging.error("Download failed. No files found.")
      
finally:
    time.sleep(10)
    driver.quit()
