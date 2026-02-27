import pandas as pd
import gspread
import logging
import os
import json
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials
from datetime import datetime

# ===============================
# VARIÁVEIS DE AMBIENTE (GitHub Secrets)
# ===============================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ABA_GOOGLE_SHEETS = 'dados_bgcard'
SENHA = os.getenv("bgcard_password")
NUM_CARTAO = os.getenv("bgcard_num")
GSERVICE_JSON = os.getenv("GSERVICE_JSON")  # JSON completo da service account

if not all([SPREADSHEET_ID, SENHA, NUM_CARTAO, GSERVICE_JSON]):
    raise ValueError("Variáveis de ambiente obrigatórias não definidas.")

# ===============================
# CONFIG CHROME (GitHub Actions)
# ===============================
chrome_options = webdriver.ChromeOptions()

# Configurações padrão do navegador
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--disable-logging")
chrome_options.add_argument("--log-level=3")  # Reduce logging
    
# Additional stability options
chrome_options.add_argument("--disable-web-security")
chrome_options.add_argument("--disable-features=VizDisplayCompositor")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
# Performance options
chrome_options.add_argument("--disable-javascript")  # Disable if not needed
chrome_options.add_argument("--disable-images")  # Disable images for speed

prefs = {
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# ===============================
# FILIAIS
# ===============================
cnpjs = {
    '06374592000198': '1',
    '06374592000279': '2',
}

# ===============================
# FUNÇÕES AUXILIARES
# ===============================
def limpar_texto(texto):
    return texto.replace('PARCELA:', '').replace('TOTAL:', '').replace('R$', '').replace('.', '').replace(',', '.').strip()

def extrair_dados_cliente(texto):
    partes = texto.split()
    cpf = partes[-3]
    parcela = partes[-1].replace("(", "").replace(")", "").replace("|", "/")
    nome = " ".join(partes[:-3])
    return nome, cpf, parcela

def inserir_dados_google_sheets(df):
    print("Conectando ao Google Sheets...")

    creds_dict = json.loads(GSERVICE_JSON)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    cliente = gspread.authorize(creds)
    planilha = cliente.open_by_key(SPREADSHEET_ID)
    worksheet = planilha.worksheet(ABA_GOOGLE_SHEETS)

    existing_values = worksheet.get_all_values()
    next_row = len(existing_values) + 1

    df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')

    data = []
    for row in df.itertuples(index=False):
        data.append([
            row.Data.strftime('%d/%m/%Y') if pd.notnull(row.Data) else "",
            str(row.CPF),
            str(row.Cliente),
            int(row.Filial) if pd.notnull(row.Filial) else "",
            str(row.Parcela),
            float(row.Valor_Parcela),
            float(row.Valor_Total)
        ])

    worksheet.update(f'A{next_row}', data, value_input_option='USER_ENTERED')
    print("Dados inseridos com sucesso!")

# ===============================
# FUNÇÃO PRINCIPAL
# ===============================
def gerar_relatorio(data_inicio, data_fim):
    navegador = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(navegador, 60)
    dados_filiais = []

    for cnpj, num_filial in cnpjs.items():
        try:
            navegador.get('https://vitrinebage.com.br/BG2024/vendas/')

            wait.until(EC.element_to_be_clickable((By.ID, "cartao"))).send_keys(NUM_CARTAO)
            wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@type="submit"]'))).click()

            wait.until(EC.element_to_be_clickable((By.XPATH, '//img'))).click()

            wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@type="text"]'))).send_keys(cnpj)
            wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@type="password"]'))).send_keys(SENHA)
            wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@type="submit"]'))).click()

            navegador.switch_to.window(navegador.window_handles[-1])

            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/table[2]/tbody/tr[1]/td/form/table[2]/tbody/tr/td[5]/a'))).click()

            campo_data_inicio = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="f_date1"]')))
            campo_data_inicio.send_keys(data_inicio)

            campo_data_fim = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="f_date2"]')))
            campo_data_fim.send_keys(data_fim)

            wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/table[2]/tbody/tr[1]/td/form/table[3]/tbody/tr[3]/td[3]/input'))).click()

            try:
                table_xpath = '//*[@id="table3"]/tbody/tr[3]/td/div/center/table/tbody/tr/td/center/center/table/tbody/tr[2]/td/center/table/tbody/tr/td/center/table/tbody/tr[2]/td/table[1]/tbody/tr[3]/td/table'
                table = navegador.find_element(By.XPATH, table_xpath)
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

                    except Exception as e:
                        logging.warning(f'Erro ao processar linha na filial {num_filial}: {e}')
                        continue

            df = pd.DataFrame(data, columns=['Filial', 'Cliente', 'CPF', 'Valor Parcela', 'Valor Total', 'Parcela', 'Data'])
            dados_filiais.append(df)

        except Exception as e:
            logging.error(f"Erro na filial {cnpj}: {e}")

    navegador.quit()

    if dados_filiais:
        df_completo = pd.concat(dados_filiais, ignore_index=True)

        df_completo.columns = (
            df_completo.columns.str.strip()
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace(' ', '_')
        )

        df_completo['Data'] = pd.to_datetime(df_completo['Data'], format='%d/%m/%Y', errors='coerce')
        df_completo = df_completo.sort_values(by=['Data', 'Cliente'])
        df_completo['Data'] = df_completo['Data'].dt.strftime('%d/%m/%Y')

        inserir_dados_google_sheets(df_completo)

# ===============================
# EXECUÇÃO
# ===============================
if __name__ == "__main__":
    hoje = datetime.today().strftime('%d/%m/%Y')
    gerar_relatorio(hoje, hoje)
