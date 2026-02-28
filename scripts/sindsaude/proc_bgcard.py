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
def get_all_files(directory=".", extensions=("csv", "xlsx")):
    """Return list of all files with given extensions in directory, sorted by modification time (oldest first)."""
    files = []
    for ext in extensions:
        files.extend(glob.glob(os.path.join(directory, f"*.{ext}")))
    return sorted(files, key=os.path.getmtime) if files else []


def clean_transfer_file(file_path: str) -> pd.DataFrame:
    logging.info(f"Reading: {os.path.basename(file_path)}")
    
    # Determine file type by extension
    if file_path.endswith('.csv'):
        # Read CSV file with comma separator
        df = pd.read_csv(file_path, sep=',', decimal=',', thousands='.')
        
        # Rename columns to match expected format
        column_mapping = {
            'Filial': 'FILIAL',
            'Cliente': 'CLIENTE',
            'CPF': 'CPF',
            'Valor Parcela': 'VALOR PARCELA',
            'Valor Total': 'VALOR TOTAL',
            'Parcela': 'PARCELA',
            'Data Venda': 'DATA'
        }
        df = df.rename(columns=column_mapping)
        
        # Clean and convert value columns
        # Remove quotes if present and convert to float
        for col in ['VALOR PARCELA', 'VALOR TOTAL']:
            if col in df.columns:
                # Remove quotes and convert comma to dot for decimal
                df[col] = df[col].astype(str).str.replace('"', '').str.replace('.', '').str.replace(',', '.').astype(float)
        
        # Ensure PARCELA is treated as string
        if 'PARCELA' in df.columns:
            df['PARCELA'] = df['PARCELA'].astype(str)
        
        # Convert DATA to datetime and then to required format
        if 'DATA' in df.columns:
            df['DATA'] = pd.to_datetime(df['DATA'], format='%d/%m/%Y', errors='coerce')
        
        # Reorder columns to match expected structure
        expected_columns = ['DATA', 'CPF', 'CLIENTE', 'FILIAL', 'PARCELA', 'VALOR PARCELA', 'VALOR TOTAL']
        df = df[expected_columns]
        
    else:
        raise ValueError(f"Unsupported file format: {file_path}")
    
    return df

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

    update_worksheet(df, sheet_id, "dados_bgcard", client)


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    sheet_id = os.getenv("SPREADSHEET_ID")
    if not sheet_id:
        logging.error("Environment variable 'SPREADSHEET_ID' not set.")
        return

    download_dir = "/home/runner/work/convenios_2/convenios_2"  # adjust as needed

    all_files = get_all_files(directory=download_dir, extensions=("csv", "xlsx"))
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
