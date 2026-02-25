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
    Load one .xls/.xlsx and produce:
    ['Filial', 'Cliente', 'CPF', 'Valor']
    """
    logging.info(f"Reading: {os.path.basename(file_path)}")

    # Read as-is; force CPF column to string later (name may vary), so keep default here
    df = pd.read_excel(file_path, header=0, dtype={"CPF": str})

    # Drop unnecessary columns
    df = df.drop(columns=[
        'CNPJ', 'Razão social', 'Matricula', 'Categoria',
        'Código', 'Descrição', 'Mês',
        'Ano', 'Data início do período', 'Data fim do período'
    ], errors="ignore")

    # ---- CPF formatting ----
    df["CPF"] = df["CPF"].str.replace(r"\D", "", regex=True)
    df["CPF"] = df["CPF"].str.zfill(11)
    df["CPF"] = df["CPF"].str.replace(
        r"(\d{3})(\d{3})(\d{3})(\d{2})",
        r"\1.\2.\3-\4",
        regex=True
    )

    out = df.rename(columns={
        df.columns[0]: 'Cliente',
        df.columns[1]: 'CPF',
        df.columns[2]: 'Valor',
    })

    logging.info(f"  -> {os.path.basename(file_path)}: {len(out)} clean rows")
    return out

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

    update_worksheet(df, sheet_id, "dados_minerva_sg", client)


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
