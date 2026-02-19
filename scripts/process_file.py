import os
import glob
import json
import time
import logging
import pandas as pd
import numpy as np
import re
import gspread

from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

# -------------------------------------------------
# Config logging
# -------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -------------------------------------------------
# File utils
# -------------------------------------------------
def get_all_files(directory=".", extensions=("xls", "xlsx")):
    """Return list of all files with given extensions in directory, sorted by modification time (oldest first)."""
    files = []
    for ext in extensions:
        files.extend(glob.glob(os.path.join(directory, f"*.{ext}")))
    return sorted(files, key=os.path.getmtime) if files else []


# -------------------------------------------------
# Google API retry helper
# -------------------------------------------------
def retry_api_call(func, retries=3, delay=2):
    """Retry a Google API call on HTTP 500 errors."""
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            status = getattr(getattr(error, "resp", None), "status", None)
            if status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")


def clean_transfer_file(file_path: str) -> pd.DataFrame:
    """
    Load one .xls/.xlsx and produce the clean dataframe:
    ['Filial', 'Data Emissão', 'Cliente', 'Parcela', 'Valor']
    """
    logging.info(f"Reading: {os.path.basename(file_path)}")

    # Carregar o arquivo Excel
    df = pd.read_excel(file_path, skiprows=9, header=0)

    # Limpar colunas desnecessárias
    df = df.drop(columns=['Unnamed: 0', 'Vencto.', 'Unnamed: 3', 'Unnamed: 4',
           'Atraso', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8',
           'Unnamed: 10', 'Unnamed: 11', 'Data    Recebe',
           'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16', 'Unnamed: 18',
           'Unnamed: 19', 'Unnamed: 21', 'Unnamed: 22',
           'Unnamed: 23', '  Vlr. Desc. ', 'Unnamed: 25', 'Unnamed: 26',
           'Unnamed: 27', 'Unnamed: 28', 'Juros Rec.', 'Unnamed: 30',
           'Unnamed: 31', 'Unnamed: 32', 'Multa Rec.', 'Unnamed: 34',
           'Unnamed: 35', 'Unnamed: 36', 'Unnamed: 37', 'Caixa', 'Unnamed: 39',
           'Fil. Rec.', 'Unnamed: 41', 'Fil.', 'Unnamed: 43', 'Venda',
           'Unnamed: 45', 'Unnamed: 46', 'Unnamed: 47', 'Cupom', 'Unnamed: 49',
           'Unnamed: 50', 'Unnamed: 51', 'Unnamed: 52', 'Unnamed: 53',
           'Dependente', 'Unnamed: 55', 'Unnamed: 56', 'Fatura'], errors="ignore")

    # Remover linhas completamente vazias
    df.dropna(how='all', inplace=True)

    # Ajustar a coluna Descrição
    df["Descrição"] = df["Descrição"].shift(-1)
    df.dropna(how='all', inplace=True)

    # Formatar data
    df['Emissão'] = pd.to_datetime(df['Emissão']).dt.strftime('%d/%m/%Y')

    # --- TRANSFORMAÇÃO PARA O FORMATO Planilha1 ---

    # Identificar a coluna de valor - pode ter nome diferente
    # Vamos procurar por colunas que contenham 'Vlr' no nome
    colunas_valor = [col for col in df.columns if 'Vlr' in str(col) or 'Rec' in str(col)]

    # Se encontrou alguma coluna de valor, usar a primeira
    if colunas_valor:
        coluna_valor = colunas_valor[0]
    else:
        # Se não encontrar, usar a última coluna (que geralmente é a de valor)
        coluna_valor = df.columns[-1]

    # Inicializar colunas vazias para Filial e Cliente
    df['Filial'] = np.nan
    df['Cliente'] = np.nan

    # Variáveis para armazenar o último Filial e Cliente encontrados
    current_filial = np.nan
    current_cliente = np.nan

    # Percorrer o dataframe para preencher Filial e Cliente
    for idx, row in df.iterrows():
        # Verificar se a linha contém "Filial:" na coluna A (Unnamed: 1)
        if pd.notna(row.get('Unnamed: 1', '')) and 'Filial:' in str(row.get('Unnamed: 1', '')):
            current_filial = row.get('Unnamed: 12', np.nan)  # O valor da filial está na coluna C
            df.at[idx, 'Filial'] = current_filial
        
        # Verificar se a linha contém "Cliente:" na coluna A
        elif pd.notna(row.get('Unnamed: 1', '')) and 'Cliente:' in str(row.get('Unnamed: 1', '')):
            current_cliente = row.get('Unnamed: 12', np.nan)  # O valor do cliente está na coluna C
            df.at[idx, 'Cliente'] = current_cliente
        
        # Para linhas de parcelas (onde Unnamed:1 é vazio ou NaN)
        else:
            df.at[idx, 'Filial'] = current_filial
            df.at[idx, 'Cliente'] = current_cliente

    # Remover linhas que não são parcelas (linhas com "Filial:" e "Cliente:")
    df_parcelas = df[df['Filial'].notna() & df['Cliente'].notna()].copy()

    # Remover as linhas originais de "Filial:" e "Cliente:" que não têm dados de parcela
    df_parcelas = df_parcelas[df_parcelas['Emissão'].notna()]

    # Selecionar as colunas disponíveis
    colunas_disponiveis = df_parcelas.columns.tolist()
    colunas_desejadas = ['Filial', 'Emissão', 'Cliente', 'Descrição', coluna_valor]

    # Verificar quais colunas desejadas estão disponíveis
    colunas_presentes = [col for col in colunas_desejadas if col in colunas_disponiveis]

    # Selecionar apenas as colunas disponíveis
    df_final = df_parcelas[colunas_presentes].copy()

    # Renomear as colunas conforme o padrão da Planilha1
    novos_nomes = {}
    mapeamento = {
        'Filial': 'Filial',
        'Emissão': 'Data Emissão',
        'Cliente': 'Cliente',
        'Descrição': 'Parcela'
    }
    # Adicionar a coluna de valor ao mapeamento
    mapeamento[coluna_valor] = 'Valor'

    # Aplicar o mapeamento apenas para colunas que existem
    for col_antiga, col_nova in mapeamento.items():
        if col_antiga in df_final.columns:
            novos_nomes[col_antiga] = col_nova

    df_final = df_final.rename(columns=novos_nomes)

    def extrair_numero_filial(texto):
        if pd.isna(texto):
            return None
        match = re.search(r'F(\d+)', str(texto))
        if match:
            return int(match.group(1))
        return None

    # Aplicar à coluna 'Filial' existente
    df_final['Filial'] = df_final['Filial'].apply(extrair_numero_filial)

    # Resetar o índice
    df_final.reset_index(drop=True, inplace=True)
    
    logging.info(f"  -> {os.path.basename(file_path)}: {len(df_final)} clean rows")
    
    return df_final


# -------------------------------------------------
# Google Sheets update
# -------------------------------------------------
def update_worksheet(df: pd.DataFrame, sheet_id: str, worksheet_name: str, client: gspread.Client):
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=max(1, len(df.columns)))
        logging.info(f"Worksheet '{worksheet_name}' created.")

    # Clear existing data and update with new data
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info(f"Updated '{worksheet_name}' with {len(df)} rows.")


def update_google_sheet(df: pd.DataFrame, sheet_id: str):
    """Authorize and update the Google Sheet."""
    logging.info("Loading Google credentials...")

    creds_env = os.getenv("GSERVICE_JSON")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_env:
        creds = Credentials.from_service_account_info(json.loads(creds_env), scopes=scope)
    else:
        creds = Credentials.from_service_account_file("notas-transf.json", scopes=scope)

    client = gspread.authorize(creds)

    update_worksheet(df, sheet_id, "dados_trier", client)


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    sheet_id = os.getenv("SPREADSHEET_ID")
    if not sheet_id:
        logging.error("Environment variable 'sheet_id' not set.")
        return

    download_dir = "/home/runner/work/convenios_2/convenios_2"  # adjust as needed

    all_files = get_all_files(directory=download_dir, extensions=("xls", "xlsx"))
    if not all_files:
        logging.warning("No Excel files found in the directory.")
        return

    logging.info(f"Found {len(all_files)} file(s) to process.")

    dfs = []
    for f in all_files:
        try:
            cleaned = clean_transfer_file(f)
            if len(cleaned) > 0:
                dfs.append(cleaned)
        except Exception as e:
            logging.exception(f"Failed processing {os.path.basename(f)}: {e}")

    if not dfs:
        logging.warning("No dataframes produced after cleaning. Nothing to upload.")
        return

    final_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Final combined rows: {len(final_df)}")

    update_google_sheet(final_df, sheet_id)


if __name__ == "__main__":
    main()
