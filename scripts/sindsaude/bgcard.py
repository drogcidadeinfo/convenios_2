import os
import time
import logging
import shutil
import pandas as pd
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
bgcard_num = os.geten("bgcard_num")
bgcard_password = os.geten("bgcard_password")
fl1 = os.geten("FL1")
fl2 = os.geten("FL2")

if not bgcard_num or not bgcard_password:
    raise ValueError("Environment variables 'bgcard_num' and/or 'bgcard_password' not set.")

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

# FILIAIS
cnpjs = {
    fl1 : '1',
    fl2 : '2',
}

# === FUNÇÕES AUXILIARES ===
def limpar_texto(texto):
    return texto.replace('PARCELA:', '').replace('TOTAL:', '').replace('R$', '').replace('.', ',').strip()

def extrair_dados_cliente(texto):
    partes = texto.split()
    cpf = partes[-3]
    parcela = partes[-1].replace("(", "").replace(")", "").replace("|", "/")
    nome = " ".join(partes[:-3])
    return nome, cpf, parcela

all_data = []
     
for cnpj, num_filial in cnpjs.items():
        try:
            logging.info('Acessando o Menu BGCard.')
            wait = WebDriverWait(driver, 60)
            driver.get('https://vitrinebage.com.br/BG2024/vendas/')

            logging.info('Realizando login no BGCard.')
            wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="cartao"]'))).send_keys(bgcard_num)
            wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="formLogin"]/table/tbody/tr/td/table/tbody/tr[3]/td/center/input'))).click()
            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/table[2]/tbody/tr[1]/td[3]/a/img'))).click()

            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/center/table[2]/tbody/tr/td[1]/table/tbody/tr[1]/td/form/table/tbody/tr/td/table/tbody/tr[3]/td/center/strong/font/input'))).send_keys(cnpj)
            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/center/table[2]/tbody/tr/td[1]/table/tbody/tr[1]/td/form/table/tbody/tr/td/table/tbody/tr[5]/td/center/input'))).send_keys(bgcard_password)
            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/center/table[2]/tbody/tr/td[1]/table/tbody/tr[1]/td/form/table/tbody/tr/td/table/tbody/tr[6]/td/center/input'))).click()

            wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="senharelatorio"]'))).send_keys(bgcard_password)
            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/table[2]/tbody/tr/td/table/tbody/tr/td/form/label/div/input[2]'))).click()

            driver.switch_to.window(driver.window_handles[-1])

            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/table[2]/tbody/tr[1]/td/form/table[2]/tbody/tr/td[5]/a'))).click()

            campo_data_inicio = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="f_date1"]')))
            campo_data_inicio.send_keys("01/02/2026")

            campo_data_fim = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="f_date2"]')))
            campo_data_fim.send_keys("27/02/2026")

            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/table[2]/tbody/tr[1]/td/form/table[3]/tbody/tr[3]/td[3]/input'))).click()

            try:
                table_xpath = '//*[@id="table3"]/tbody/tr[3]/td/div/center/table/tbody/tr/td/center/center/table/tbody/tr[2]/td/center/table/tbody/tr/td/center/table/tbody/tr[2]/td/table[1]/tbody/tr[3]/td/table'
                table = driver.find_element(By.XPATH, table_xpath)
            except Exception as e:
                logging.warning(f'Tabela não encontrada para filial {num_filial}: {e}')
                continue

            rows = table.find_elements(By.TAG_NAME, 'tr')
            data = []

            for i in range(len(rows)):
                cols = table.find_elements(By.TAG_NAME, 'tr')[i].find_elements(By.TAG_NAME, 'td')
                if len(cols) == 8:
                    try:
                        cliente_completo = cols[2].text
                        nome_cliente, cpf_cliente, parcela_cliente = extrair_dados_cliente(cliente_completo)
                        valor_parcela = limpar_texto(cols[6].text)
                        valor_total = limpar_texto(cols[7].text)

                        # 🔥 NOVO: Capturar a Data no 6º TD (índice 5)
                        data_texto = cols[5].text
                        import re
                        match = re.search(r'(\d{2}/\d{2}/\d{4})', data_texto)
                        data_venda = match.group(1) if match else ''

                        data.append([num_filial, nome_cliente.strip(), cpf_cliente.strip(),
                                        valor_parcela, valor_total, parcela_cliente, data_venda])
                        
                        # Adiciona à lista global (TODAS as filiais)
                        all_data.append([num_filial, nome_cliente.strip(), cpf_cliente.strip(),
                                        valor_parcela, valor_total, parcela_cliente, data_venda])

                    except Exception as e:
                        logging.warning(f'Erro ao processar linha na filial {num_filial}: {e}')
                        continue
                    
            logging.info(f"Dados coletados para filial {num_filial}: {len(data)} registros")

        except Exception as e:
            logging.error(f'Ocorreu um erro para a filial {cnpj}: {e}')
            
if all_data:
    df = pd.DataFrame(all_data, columns=['Filial', 'Cliente', 'CPF', 
                                        'Valor Parcela', 'Valor Total', 
                                        'Parcela', 'Data Venda'])
    
    logging.info(f"Total de registros: {len(df)}")
    
    # Salvar como CSV
    filename = f'dados_bgcard.csv'
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logging.info(f"\n✅ Arquivo salvo: {filename}")
    
    # Estatísticas por filial
    logging.info("\n📊 Resumo por filial:")
    for filial in df['Filial'].unique():
        qtd = len(df[df['Filial'] == filial])
        logging.info(f"  Filial {filial}: {qtd} registros")
else:
    print("\n❌ Nenhum dado encontrado!")
            
