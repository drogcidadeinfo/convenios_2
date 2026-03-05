import re
import os
import json
import unicodedata
from datetime import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Config
# ----------------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

SHEET_TRIER = "dados_trier_sind"
SHEET_BGCARD = "dados_bgcard"
SHEET_OUT = "TRIERxSIND"

HEADER = [
    "Filial",
    "Data Emissão",
    "TRIER",
    "Valor Parcela",
    "Valor Total",
    "BGCARD",
    "Valor Parcela",
    "Valor Total",
    "STATUS"
]

VALUE_TOL = 0.75   # tolerância

# Column mappings (original -> normalized)
COLUMN_MAPPING = {
    'filial': 'filial',
    'data': 'data',
    'cliente': 'cliente',
    'cpf': 'cpf',
    'parcela': 'parcela',
    'valor parcela': 'valor_parcela',
    'valor total': 'valor_total'
}

# ----------------------------
# Helpers
# ----------------------------
def normalize_colname(x):
    """Normalize column name to lowercase, remove accents and extra spaces."""
    s = str(x).replace("\xa0", " ").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).replace(' ', '_')  # Replace spaces with underscores


def normalize_df_columns(df):
    """Normalize DataFrame columns and map to expected names."""
    df = df.copy()
    
    # First normalize all column names
    normalized_cols = [normalize_colname(c) for c in df.columns]
    df.columns = normalized_cols
    
    # Then map to expected column names if they match our patterns
    expected_cols = {}
    for norm_col in normalized_cols:
        for orig, norm in COLUMN_MAPPING.items():
            if norm in norm_col or orig in norm_col:
                expected_cols[norm] = norm_col
                break
    
    return df, expected_cols


def parse_date_br(x):
    """Parse Brazilian date format."""
    if not x or pd.isna(x):
        return None
    try:
        d = pd.to_datetime(x, dayfirst=True, errors="coerce")
        return None if pd.isna(d) else d.date()
    except:
        return None


def parse_parcela(x):
    """
    Parse parcel format like "1/5" -> (1, 5)
    """
    if not x or pd.isna(x):
        return (None, None)

    m = re.search(r"(\d+)\s*/\s*(\d+)", str(x))
    if not m:
        return (None, None)

    return int(m.group(1)), int(m.group(2))


def clean_cpf(x):
    """Remove non-digits from CPF."""
    if pd.isna(x):
        return ""
    return re.sub(r"\D", "", str(x))


def safe_get_worksheet(sh, sheet_name):
    """Safely get worksheet, return None if not found."""
    try:
        return sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        print(f"Warning: Worksheet '{sheet_name}' not found")
        return None


# ----------------------------
# Core logic
# ----------------------------
def build_rows(df_trier, df_bg):
    """Build comparison rows between TRIER and BGCARD data."""
    
    # Handle empty DataFrames
    if df_trier.empty and df_bg.empty:
        return []
    
    # Normalize DataFrames and get column mappings
    t, t_cols = normalize_df_columns(df_trier)
    b, b_cols = normalize_df_columns(df_bg)
    
    # Check if required columns exist
    required_cols = ['cpf', 'data', 'cliente', 'parcela', 'valor_parcela', 'valor_total', 'filial']
    
    for df_name, df, col_map in [("TRIER", t, t_cols), ("BGCARD", b, b_cols)]:
        missing = [col for col in required_cols if col not in col_map]
        if missing and not df.empty:
            print(f"Warning: {df_name} missing columns: {missing}")
            # Add empty columns for missing ones to avoid KeyError
            for col in missing:
                df[col] = None

    # Standardize columns using mapped names
    for df, col_map in [(t, t_cols), (b, b_cols)]:
        if not df.empty:
            # Use mapped column names where available
            df['cpf'] = df[col_map.get('cpf', 'cpf')].apply(clean_cpf)
            
            # Parse date
            data_col = col_map.get('data', 'data')
            if data_col in df.columns:
                df['data_emissao'] = df[data_col].apply(parse_date_br)
            else:
                df['data_emissao'] = None
            
            # Parse parcela
            parcela_col = col_map.get('parcela', 'parcela')
            if parcela_col in df.columns:
                df[['parcela_n', 'parcela_total']] = df[parcela_col].apply(
                    lambda x: pd.Series(parse_parcela(x))
                )
            else:
                df['parcela_n'] = None
                df['parcela_total'] = None
            
            # Get numeric values
            valor_parcela_col = col_map.get('valor_parcela', 'valor_parcela')
            valor_total_col = col_map.get('valor_total', 'valor_total')
            
            # Convert to float, handling currency strings
            df['valor_parcela_num'] = pd.to_numeric(
                df[valor_parcela_col].astype(str).str.replace('R\$', '', regex=True)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .str.strip(), 
                errors='coerce'
            )
            
            df['valor_total_num'] = pd.to_numeric(
                df[valor_total_col].astype(str).str.replace('R\$', '', regex=True)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .str.strip(),
                errors='coerce'
            )
            
            # Get cliente name
            cliente_col = col_map.get('cliente', 'cliente')
            df['cliente_name'] = df[cliente_col].astype(str).str.strip()
            
            # Get filial
            filial_col = col_map.get('filial', 'filial')
            df['filial'] = df[filial_col]

    used_bg = set()
    out = []

    # Process TRIER rows
    if not t.empty:
        for i, row_t in t.iterrows():

            match_index = None
            best_match = None
            best_diff = float('inf')

            for j, row_b in b.iterrows():

                if j in used_bg:
                    continue

                # Skip if either has missing required data
                if pd.isna(row_t.get('cpf')) or pd.isna(row_b.get('cpf')):
                    continue

                # CPF must match
                if str(row_t['cpf']) != str(row_b['cpf']):
                    continue

                # parcela number must match (if both have it)
                if not pd.isna(row_t.get('parcela_n')) and not pd.isna(row_b.get('parcela_n')):
                    if row_t['parcela_n'] != row_b['parcela_n']:
                        continue

                # compare values with tolerance
                if not pd.isna(row_t.get('valor_parcela_num')) and not pd.isna(row_b.get('valor_parcela_num')):
                    diff = abs(row_t['valor_parcela_num'] - row_b['valor_parcela_num'])
                    
                    if diff <= VALUE_TOL:
                        # Check total as well
                        total_diff = abs(row_t.get('valor_total_num', 0) - row_b.get('valor_total_num', 0))
                        if total_diff <= VALUE_TOL:
                            match_index = j
                            break
                        elif diff < best_diff:
                            best_diff = diff
                            best_match = j

            # If exact match not found but we have a best match within tolerance
            if match_index is None and best_match is not None:
                match_index = best_match

            # ---------- MATCH FOUND ----------
            if match_index is not None:

                used_bg.add(match_index)
                row_b = b.loc[match_index]

                # Format names safely
                trier_cliente = str(row_t.get('cliente_name', '')).strip()
                bg_cliente = str(row_b.get('cliente_name', '')).strip()
                
                trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
                bg_parcela = f"{row_b.get('parcela_n', '?')}/{row_b.get('parcela_total', '?')}"
                
                trier_name = f"{trier_cliente} — PARCELA {trier_parcela}"
                bg_name = f"{bg_cliente} — PARCELA {bg_parcela}"

                # check total parcelas
                if (not pd.isna(row_t.get('parcela_total')) and 
                    not pd.isna(row_b.get('parcela_total')) and
                    row_t['parcela_total'] != row_b['parcela_total']):
                    status = "⚠️ NUM DE PARCELAS DIVERGENTES"
                else:
                    status = "✅ OK"

                out.append([
                    row_t.get('filial', ''),
                    row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                    trier_name,
                    row_t.get('valor_parcela_num', 0),
                    row_t.get('valor_total_num', 0),
                    bg_name,
                    row_b.get('valor_parcela_num', 0),
                    row_b.get('valor_total_num', 0),
                    status
                ])

            # ---------- ONLY TRIER ----------
            else:
                trier_cliente = str(row_t.get('cliente_name', '')).strip()
                trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
                trier_name = f"{trier_cliente} — PARCELA {trier_parcela}"

                out.append([
                    row_t.get('filial', ''),
                    row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                    trier_name,
                    row_t.get('valor_parcela_num', 0),
                    row_t.get('valor_total_num', 0),
                    "-",
                    "-",
                    "-",
                    "⚠️ SOMENTE TRIER"
                ])

    # ---------- ONLY BGCARD ----------
    if not b.empty:
        for j, row_b in b.iterrows():
            if j in used_bg:
                continue

            bg_cliente = str(row_b.get('cliente_name', '')).strip()
            bg_parcela = f"{row_b.get('parcela_n', '?')}/{row_b.get('parcela_total', '?')}"
            bg_name = f"{bg_cliente} — PARCELA {bg_parcela}"

            out.append([
                row_b.get('filial', ''),
                row_b['data_emissao'].strftime("%d/%m/%Y") if row_b.get('data_emissao') else '',
                "-",
                "-",
                "-",
                bg_name,
                row_b.get('valor_parcela_num', 0),
                row_b.get('valor_total_num', 0),
                "⚠️ SOMENTE BGCARD"
            ])

    return out


# ----------------------------
# Main
# ----------------------------
def main():
    """Main execution function."""
    
    if not SPREADSHEET_ID:
        print("Error: SPREADSHEET_ID environment variable not set")
        return

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        
        # Load credentials
        creds_json = os.environ.get("GSERVICE_JSON")
        if not creds_json:
            print("Error: GSERVICE_JSON environment variable not set")
            return
            
        creds = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=scopes
        )

        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)

        # Safely get worksheets
        ws_trier = safe_get_worksheet(sh, SHEET_TRIER)
        ws_bg = safe_get_worksheet(sh, SHEET_BGCARD)
        
        if not ws_trier or not ws_bg:
            print("Error: Required worksheets not found")
            return

        # Read data
        df_trier = pd.DataFrame(ws_trier.get_all_records())
        df_bg = pd.DataFrame(ws_bg.get_all_records())

        print(f"Read {len(df_trier)} rows from TRIER, {len(df_bg)} rows from BGCARD")

        rows = build_rows(df_trier, df_bg)

        # Get or create output worksheet
        try:
            ws_out = sh.worksheet(SHEET_OUT)
        except gspread.WorksheetNotFound:
            ws_out = sh.add_worksheet(title=SHEET_OUT, rows=2000, cols=10)
            print(f"Created new worksheet: {SHEET_OUT}")

        values = [HEADER] + rows
        ws_out.clear()
        ws_out.update("A1", values)
        
        print(f"Successfully updated {SHEET_OUT} with {len(rows)} rows")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()
