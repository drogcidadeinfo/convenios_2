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
    logging.info(f"Reading: {os.path.basename(file_path)}")

    # Carregar o arquivo Excel
    df = pd.read_excel(file_path, header=None)

    dados = []
    filial_atual = None
    cliente_atual = None
    cpf_atual = None

    # 🔍 Itera sobre as linhas do DataFrame
    for idx, row in df.iterrows():
        for col_idx, value in row.items():
            # ✅ Quando encontra "Filial:", salva como filial_atual
            if str(value).strip() == "Filial:":
                colunas = df.columns.tolist()
                pos = colunas.index(col_idx)
                if pos + 11 < len(row):
                    filial_texto = str(row.iloc[pos + 11])
                    if filial_texto.startswith("F"):
                        filial_atual = filial_texto.split()[0]  # Pega F01, F02 etc.
                        filial_atual = filial_atual.replace("F", "")  # Remove o 'F'
                        filial_atual = str(int(filial_atual))  # Remove zero à esquerda
                    else:
                        filial_atual = ""
                    print(f"[DEBUG] Linha {idx} → Filial encontrada: {filial_atual}")
                continue

            # ✅ Quando encontra "Cliente:", salva cliente e cpf
            if str(value).strip() == "Cliente:":
                colunas = df.columns.tolist()
                pos = colunas.index(col_idx)

                if pos + 11 < len(row):
                    cliente_atual = row.iloc[pos + 11]
                if pos + 34 < len(row):
                    cpf_atual = row.iloc[pos + 34]

                # 🔍 A partir do CLIENTE, buscar parcelas
                linha_parcela = idx + 1
                while linha_parcela < len(df):
                    celula_valor = df.iloc[linha_parcela, pos + 19]

                    if pd.isna(celula_valor):
                        prox_linha = linha_parcela + 1
                        if prox_linha < len(df) and pd.isna(df.iloc[prox_linha, pos + 8]):
                            break  # duas linhas vazias = acabou parcelas
                        else:
                            linha_parcela += 1
                            continue

                    valor_parcela = float(celula_valor)

                    # 📅 Captura a DATA (11 colunas à esquerda da célula do valor)
                    col_data = (pos + 19) - 11
                    if col_data >= 0:
                        data_bruta = df.iloc[linha_parcela, col_data]
                        try:
                            data_formatada = pd.to_datetime(data_bruta).strftime('%d/%m/%Y')
                        except:
                            data_formatada = str(data_bruta)
                    else:
                        data_formatada = ""

                    # ✅ Pegar PARCELA (3 células à esquerda e 1 para baixo do valor)
                    if (pos + 19 - 3) >= 0 and (linha_parcela + 1) < len(df):
                        celula_parcela = df.iloc[linha_parcela + 1, pos + 16]
                        if isinstance(celula_parcela, str) and "PARCELA" in celula_parcela:
                            parcela_num = celula_parcela.replace("PARCELA", "").strip()
                        else:
                            parcela_num = None
                    else:
                        parcela_num = None

                    # ✅ Calcula VALOR TOTAL com arredondamento
                    if parcela_num and "/" in parcela_num:
                        total_parcelas = int(parcela_num.split("/")[1])
                        valor_total = round(valor_parcela * total_parcelas, 2)
                    else:
                        valor_total = round(valor_parcela, 2)

                    filial_formatada = filial_atual
                    dados.append({
                        "DATA": pd.to_datetime(data_formatada, dayfirst=True, errors='coerce'),
                        "CPF": str(cpf_atual).strip() if cpf_atual else "",
                        "CLIENTE": str(cliente_atual) if cliente_atual else "",
                        "FILIAL": int(filial_formatada) if filial_formatada and filial_formatada.isdigit() else None,
                        "PARCELA": str(parcela_num) if parcela_num else "",
                        "VALOR PARCELA": round(float(valor_parcela), 2),
                        "VALOR TOTAL": round(float(valor_total), 2)
                    })

                    linha_parcela += 2  # pula para próxima parcela

    # ✅ Monta DataFrame final e ordena por DATA (desc) e CLIENTE (asc)
    df_result = pd.DataFrame(dados)

    # Converter a coluna DATA para datetime para ordenar corretamente
    df_result['DATA'] = pd.to_datetime(df_result['DATA'], format='%d/%m/%Y', errors='coerce')

    # Ordenar: primeiro por DATA decrescente, depois por CLIENTE ascendente
    df_result = df_result.sort_values(by=["DATA", "CLIENTE"], ascending=[True, True]).reset_index(drop=True)

    # Converter a DATA de volta para string no formato dd/mm/yyyy
    df_result['DATA'] = df_result['DATA'].dt.strftime('%d/%m/%Y')

    return df_result

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

    existing_values = ws.get_all_values()
    next_row = len(existing_values) + 1

    # Converte DATA para datetime garantindo que são objetos de data
    df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce', dayfirst=True)
    df['DATA'] = df['DATA'].apply(lambda x: x.strftime('%d/%m/%Y') if pd.notnull(x) else "")

    # Monta a lista mantendo os tipos corretos
    data = []
    for row in df.to_dict('records'):
        data.append([
            row['DATA'],
            row['CPF'],
            row['CLIENTE'],
            row['FILIAL'] if row['FILIAL'] is not None else "",
            row['PARCELA'],
            float(row['VALOR PARCELA']),
            float(row['VALOR TOTAL'])
        ])

    ws.update(f'A{next_row}', data, value_input_option='USER_ENTERED')
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

    update_worksheet(df, sheet_id, "dados_trier_sind", client)


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
