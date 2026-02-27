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
    ['Cliente', 'CPF', 'Valor']
    """
    logging.info(f"Reading: {os.path.basename(file_path)}")

    # Read as-is; force CPF column to string later (name may vary), so keep default here
    df = pd.read_excel(file_path, skiprows=9, header=0)

    df = df.drop(columns=[
        'Unnamed: 0', 'Vencto.', 'Unnamed: 3', 'Unnamed: 4',
       'Atraso', 'Unnamed: 6', 'Unnamed: 8',
       'Unnamed: 10', 'Unnamed: 11', 'Data    Recebe', 'Emissão',
       'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16', 'Descrição',
       'Unnamed: 19', 'Vlr. Rec. c/ ', 'Unnamed: 21', 'Unnamed: 22',
       'Unnamed: 23', '  Vlr. Desc. ', 'Unnamed: 25', 'Unnamed: 26',
       'Unnamed: 27', 'Unnamed: 28', 'Juros Rec.', 'Unnamed: 30',
       'Unnamed: 31', 'Unnamed: 32', 'Multa Rec.', 'Unnamed: 34',
       'Unnamed: 36', 'Unnamed: 37', 'Caixa', 'Unnamed: 39',
       'Fil. Rec.', 'Unnamed: 41', 'Fil.', 'Unnamed: 43', 'Venda',
       'Unnamed: 45', 'Unnamed: 46', 'Unnamed: 47', 'Cupom', 'Unnamed: 49',
       'Unnamed: 50', 'Unnamed: 51', 'Unnamed: 52', 'Unnamed: 53',
       'Dependente', 'Unnamed: 55', 'Unnamed: 56', 'Fatura'
    ], errors="ignore")

    df.dropna(how="all", inplace=True)
    df = df.reset_index(drop=True)

    # ---------- move Unnamed: 18 up 1 row (your code does -1) ----------
    col = "Unnamed: 18"
    if col in df.columns:
        src = df[col].notna().values
        src_idx = np.flatnonzero(src)
        dst_idx = src_idx - 1  # up 1 row
        valid = dst_idx >= 0
        df.loc[dst_idx[valid], col] = df.loc[src_idx[valid], col].values
        df.loc[src_idx[valid], col] = np.nan

    df = df.drop(columns=['Unnamed: 1', 'Unnamed: 7'], errors="ignore")
    df.dropna(how='all', inplace=True)
    
    df = df.rename(columns={
        df.columns[0]: 'Nome',
        df.columns[1]: 'Valor',
        df.columns[2]: 'CPF',
    })
    
    cpf_col    = "CPF" 

    # CPF: force digits + zfill + format
    if cpf_col in df.columns:
        df["CPF"] = (
            df[cpf_col]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .str.zfill(11)
            .str.replace(r"(\d{3})(\d{3})(\d{3})(\d{2})", r"\1.\2.\3-\4", regex=True)
        )
    else:
        df["CPF"] = pd.NA

    # Valor: try to convert pt-BR "269,09" to float
    valor_col  = "Valor" 
    
    if valor_col in df.columns:
        raw = df[valor_col].astype(str).str.strip()
    
        has_comma = raw.str.contains(",", regex=False, na=False)
    
        # looks like plain number with dot decimal (e.g. "207.0", "28.0", "207.28")
        is_dot_decimal = raw.str.match(r"^\d+(\.\d+)?$", na=False)
    
        # Parse values that are dot-decimals directly (do NOT remove dot)
        val_dot = pd.to_numeric(raw.where(is_dot_decimal), errors="coerce").astype("float64")
    
        # For the rest (non dot-decimal), normalize pt-BR thousands/decimal
        normalized = (
            raw
            .str.replace("R$", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(".", "", regex=False)   # remove thousands separator
            .str.replace(",", ".", regex=False)  # decimal comma -> dot
        )
        val_other = pd.to_numeric(normalized.where(~is_dot_decimal), errors="coerce").astype("float64")
    
        # Combine
        val = val_dot.combine_first(val_other)
    
        # Centavos heuristic ONLY when:
        # - no comma
        # - not dot-decimal
        # - digits length >= 4  (e.g. "26909" -> 269.09)
        digits_len = raw.str.replace(r"\D", "", regex=True).str.len()
        is_centavos = (~has_comma) & (~is_dot_decimal) & (digits_len >= 4)
    
        val = np.where(is_centavos, val / 100.0, val)
    
        df["Valor"] = pd.Series(val, index=df.index)
    
        # Format to pt-BR string
        df["Valor"] = df["Valor"].map(
            lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            if pd.notna(x) else ""
        )
    else:
        df["Valor"] = pd.NA

    out = df[["Filial", "Cliente", "CPF", "Valor"]]

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

    # Clear existing data
    ws.clear()
    
    # Update with values (as numbers)
    ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")
    
    # Apply Contabilidade format to the Valor column (column C, index 3)
    # Column indices: A=1, B=2, C=3 (Valor)
    if len(df) > 0:
        # Find the Valor column index (it's the 4th column in your output)
        valor_col_idx = 3  # Column C
        
        # Apply number format to all data rows in the Valor column
        # Skip header row (row 1)
        cell_range = f"C2:C{len(df) + 1}"
        
        # Format as Contabilidade (R$ #.##0,00;R$ -#.##0,00)
        ws.format(cell_range, {
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": 'R$ #.##0,00;R$ -#.##0,00'
            }
        })
        
        logging.info(f"Applied Contabilidade format to {cell_range}")
    
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

    update_worksheet(df, sheet_id, "dados_trier_alegrete", client)


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
