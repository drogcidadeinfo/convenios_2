import os
import json
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials
import gspread

# -------------------------------------------------
# Config logging
# -------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# === CONFIGURAÇÕES ===
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ABA_GOOGLE_SHEETS = 'dados_bgcard'
SENHA = os.getenv("bgcard_password")
NUM_CARTAO = os.getenv("bgcard_num")

# FILIAIS
cnpjs = {
    '06374592000198': '1',
    '06374592000279': '2',
}

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


# === FUNÇÕES AUXILIARES ===
def limpar_texto(texto):
    """Remove labels e formata texto para extração de números"""
    texto = texto.replace('PARCELA:', '').replace('TOTAL:', '').replace('R$', '').strip()
    texto = texto.replace('.', '').replace(',', '.')  # Converte para formato numérico
    return texto


def extrair_dados_cliente(texto):
    """Extrai nome, CPF e parcela do texto do cliente"""
    partes = texto.split()
    cpf = partes[-3]
    parcela = partes[-1].replace("(", "").replace(")", "").replace("|", "/")
    nome = " ".join(partes[:-3])
    return nome, cpf, parcela

'''def parse_brl_money(x):
    """Converte valor em formato BRL para float"""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s == "-":
        return None
    # remove "R$", espaços e etc
    s = s.replace("R$", "").replace(" ", "")
    # milhar '.' e decimal ','
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def format_brl(v):
    """Formata valor float para formato BRL"""
    if v is None or pd.isna(v):
        return "-"
    # formata 1234.5 -> "R$ 1.234,50"
    s = f"{float(v):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"'''

def parse_date_br(x):
    """Converte string dd/mm/yyyy para datetime"""
    if x is None or str(x).strip() == "":
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).date()
    s = str(x).strip()
    d = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.date()


# -------------------------------------------------
# Google Sheets update (padrão do seu código)
# -------------------------------------------------
def update_worksheet(df: pd.DataFrame, sheet_id: str, worksheet_name: str, client: gspread.Client):
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=max(1, len(df.columns)))
        logging.info(f"Worksheet '{worksheet_name}' created.")

    # Clear existing data
    ws.clear()
    
    # Prepare headers
    headers = ["Data", "CPF", "Cliente", "Filial", "Parcela", "Valor Parcela", "Valor Total"]
    
    # Prepare data with proper type handling
    data_rows = []
    for _, row in df.iterrows():
        try:
            data_rows.append([
                row['Data'] if pd.notna(row['Data']) else "",
                row['CPF'] if pd.notna(row['CPF']) else "",
                row['Cliente'] if pd.notna(row['Cliente']) else "",
                int(row['Filial']) if pd.notna(row['Filial']) else "",
                row['Parcela'] if pd.notna(row['Parcela']) else "",
                float(row['Valor Parcela']) if pd.notna(row['Valor Parcela']) else 0.0,
                float(row['Valor Total']) if pd.notna(row['Valor Total']) else 0.0
            ])
        except (ValueError, TypeError) as e:
            logging.warning(f"Skipping row due to data error: {e}")
            continue
    
    # Update with headers and data
    all_data = [headers] + data_rows
    ws.update(all_data, value_input_option='USER_ENTERED')
    
    # Format columns for better readability
    if len(data_rows) > 0:
        # Format Valor Parcela and Valor Total as currency
        ws.format('F:G', {
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": 'R$ #.##0,00;R$ -#.##0,00'
            }
        })
        
        # Format Date column
        ws.format('A:A', {
            "numberFormat": {
                "type": "DATE",
                "pattern": 'dd/mm/yyyy'
            }
        })
    
    logging.info(f"Updated '{worksheet_name}' with {len(data_rows)} rows.")


def update_google_sheet(df: pd.DataFrame, sheet_id: str):
    """Authorize and update the Google Sheet following padrão do código."""
    logging.info("Loading Google credentials...")

    creds_env = os.getenv("GSERVICE_JSON")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_env:
        creds = Credentials.from_service_account_info(json.loads(creds_env), scopes=scope)
    else:
        # Fallback for local development
        creds = Credentials.from_service_account_file("notas-transf.json", scopes=scope)

    client = gspread.authorize(creds)

    update_worksheet(df, sheet_id, ABA_GOOGLE_SHEETS, client)


# === FUNÇÃO PRINCIPAL ===
def gerar_relatorio(data_inicio, data_fim):
    logging.info(f"Iniciando geração de relatório: {data_inicio} a {data_fim}")
    
    navegador = None
    try:
        navegador = webdriver.Chrome(options=chrome_options)
        navegador.maximize_window()
        wait = WebDriverWait(navegador, 60)
        dados_filiais = []

        for cnpj, num_filial in cnpjs.items():
            try:
                logging.info(f'Processando filial {num_filial} - CNPJ: {cnpj}')
                
                logging.info('Acessando o Menu BGCard.')
                navegador.get('https://vitrinebage.com.br/BG2024/vendas/')

                logging.info('Realizando login no BGCard.')
                wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="cartao"]'))).send_keys(NUM_CARTAO)
                wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="formLogin"]/table/tbody/tr/td/table/tbody/tr[3]/td/center/input'))).click()
                wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/table[2]/tbody/tr[1]/td[3]/a/img'))).click()

                wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/center/table[2]/tbody/tr/td[1]/table/tbody/tr[1]/td/form/table/tbody/tr/td/table/tbody/tr[3]/td/center/strong/font/input'))).send_keys(cnpj)
                wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/center/table[2]/tbody/tr/td[1]/table/tbody/tr[1]/td/form/table/tbody/tr/td/table/tbody/tr[5]/td/center/input'))).send_keys(SENHA)
                wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/center/table[2]/tbody/tr/td[1]/table/tbody/tr[1]/td/form/table/tbody/tr/td/table/tbody/tr[6]/td/center/input'))).click()

                wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="senharelatorio"]'))).send_keys(SENHA)
                wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/table[2]/tbody/tr/td/table/tbody/tr/td/form/label/div/input[2]'))).click()

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
                            
                            # Process values
                            valor_parcela_str = limpar_texto(cols[6].text)
                            valor_total_str = limpar_texto(cols[7].text)
                            
                            '''valor_parcela = parse_brl_money(valor_parcela_str)
                            valor_total = parse_brl_money(valor_total_str)'''
                            
                            valor_parcela = valor_parcela_str
                            valor_total = valor_total_str

                            # Capturar a Data
                            data_texto = cols[5].text
                            match = re.search(r'(\d{2}/\d{2}/\d{4})', data_texto)
                            data_venda = match.group(1) if match else ''

                            data.append([
                                num_filial, 
                                nome_cliente.strip(), 
                                cpf_cliente.strip(),
                                valor_parcela, 
                                valor_total, 
                                parcela_cliente, 
                                data_venda
                            ])

                        except Exception as e:
                            logging.warning(f'Erro ao processar linha na filial {num_filial}: {e}')
                            continue

                df = pd.DataFrame(data, columns=['Filial', 'Cliente', 'CPF', 'Valor Parcela', 'Valor Total', 'Parcela', 'Data'])
                dados_filiais.append(df)

            except Exception as e:
                logging.error(f'Ocorreu um erro para a filial {cnpj}: {e}')

    except Exception as e:
        logging.error(f"Erro no navegador: {e}")
    finally:
        if navegador:
            navegador.quit()

    if dados_filiais:
        df_completo = pd.concat(dados_filiais, ignore_index=True)

        # Normaliza nomes das colunas
        df_completo.columns = (
            df_completo.columns.str.strip()
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace(' ', '_')
        )

        # Renomeia de volta para os nomes originais para consistência
        df_completo = df_completo.rename(columns={
            'Filial': 'Filial',
            'Cliente': 'Cliente',
            'CPF': 'CPF',
            'Valor_Parcela': 'Valor Parcela',
            'Valor_Total': 'Valor Total',
            'Parcela': 'Parcela',
            'Data': 'Data'
        })

        # Converte a coluna 'Data' para datetime
        df_completo['Data'] = pd.to_datetime(df_completo['Data'], format='%d/%m/%Y', errors='coerce')

        # Ordena pela Data e Cliente
        df_completo = df_completo.sort_values(by=['Data', 'Cliente'], ascending=[True, True])

        # Converte a Data de volta para string
        df_completo['Data'] = df_completo['Data'].dt.strftime('%d/%m/%Y')

        # Formata CPF
        df_completo['CPF'] = df_completo['CPF'].apply(
            lambda x: re.sub(r"(\d{3})(\d{3})(\d{3})(\d{2})", r"\1.\2.\3-\4", x.zfill(11)) 
            if x and len(re.sub(r"\D", "", str(x))) <= 11 else x
        )

        logging.info(f"Total de registros processados: {len(df_completo)}")
        return df_completo
    else:
        logging.warning("Nenhum dado foi coletado.")
        return pd.DataFrame()


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    # Verificar variáveis de ambiente
    if not SPREADSHEET_ID:
        logging.error("Environment variable 'SPREADSHEET_ID' not set.")
        return

    # Definir período do relatório (últimos 7 dias como exemplo)
    data_fim = datetime.now()
    data_inicio = data_fim - timedelta(days=7)
    
    data_inicio_str = data_inicio.strftime("%d/%m/%Y")
    data_fim_str = data_fim.strftime("%d/%m/%Y")

    # Gerar relatório
    df_resultado = gerar_relatorio(data_inicio_str, data_fim_str)

    if len(df_resultado) > 0:
        # Atualizar Google Sheets
        update_google_sheet(df_resultado, SPREADSHEET_ID)
        logging.info("Processo concluído com sucesso!")
    else:
        logging.warning("Nenhum dado para atualizar.")


if __name__ == "__main__":
    main()
