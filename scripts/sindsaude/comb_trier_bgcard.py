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

# Column mappings (original -> normalized) - expanded to handle variations
COLUMN_MAPPING = {
    'filial': 'filial',
    'data': 'data',
    'data emissao': 'data',
    'data_emissao': 'data',
    'cliente': 'cliente',
    'cpf': 'cpf',
    'parcela': 'parcela',
    'valor parcela': 'valor_parcela',
    'valor_parcela': 'valor_parcela',
    'valor total': 'valor_total',
    'valor_total': 'valor_total'
}

# ----------------------------
# Helpers
# ----------------------------
def normalize_colname(x):
    """Normalize column name to lowercase, remove accents and extra spaces."""
    if not isinstance(x, str):
        x = str(x)
    s = x.replace("\xa0", " ").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()


def normalize_df_columns(df):
    """Normalize DataFrame columns and map to expected names."""
    if df.empty:
        return df, {}
    
    df = df.copy()
    
    # First normalize all column names
    normalized_cols = [normalize_colname(c) for c in df.columns]
    df.columns = normalized_cols
    
    # Then map to expected column names if they match our patterns
    expected_cols = {}
    for expected, normalized in COLUMN_MAPPING.items():
        for col in normalized_cols:
            if expected in col or normalized in col:
                expected_cols[normalized] = col
                break
    
    return df, expected_cols


def parse_date_br(x):
    """Parse Brazilian date format."""
    if not x or pd.isna(x) or x == "":
        return None
    try:
        # Handle different date formats
        if isinstance(x, str):
            # Try common Brazilian formats
            for fmt in ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d']:
                try:
                    d = datetime.strptime(x, fmt)
                    return d.date()
                except ValueError:
                    continue
        d = pd.to_datetime(x, dayfirst=True, errors="coerce")
        return None if pd.isna(d) else d.date()
    except:
        return None


def parse_parcela(x):
    """
    Parse parcel format like "1/5" -> (1, 5)
    """
    if not x or pd.isna(x) or x == "":
        return (None, None)

    m = re.search(r"(\d+)\s*[\/\-]\s*(\d+)", str(x))
    if not m:
        return (None, None)

    return int(m.group(1)), int(m.group(2))


def clean_cpf(x):
    """Remove non-digits from CPF."""
    if pd.isna(x) or x == "":
        return ""
    return re.sub(r"\D", "", str(x))


def safe_float_convert(value):
    """Safely convert value to float, handling currency formats."""
    if pd.isna(value) or value == "" or value is None:
        return 0.0
    
    try:
        # If already numeric, return as float
        if isinstance(value, (int, float)):
            return float(value)
        
        # Convert to string and clean
        s = str(value).strip()
        
        # Remove currency symbol and thousand separators
        # Using raw string to avoid escape sequence warning
        s = re.sub(r'[R$\s]', '', s)  # Remove R$, spaces
        s = s.replace('.', '')  # Remove thousand separators
        s = s.replace(',', '.')  # Replace decimal comma with dot
        
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def safe_get_worksheet(sh, sheet_name):
    """Safely get worksheet, return None if not found."""
    try:
        return sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        print(f"Warning: Worksheet '{sheet_name}' not found")
        return None


def format_value_for_json(val):
    """Format value to be JSON serializable (no NaN, Infinity)."""
    if pd.isna(val) or val is None or val == "":
        return ""
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return 0.0
    if isinstance(val, (int, float)):
        # Round to 2 decimal places for currency
        return round(float(val), 2)
    return str(val)


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
    
    print(f"TRIER columns: {list(t.columns)}")
    print(f"BGCARD columns: {list(b.columns)}")
    print(f"TRIER mapping: {t_cols}")
    print(f"BGCARD mapping: {b_cols}")
    
    # Required columns with fallbacks
    required_cols = ['cpf', 'data', 'cliente', 'parcela', 'valor_parcela', 'valor_total', 'filial']
    
    # Standardize columns using mapped names
    for df_name, df, col_map in [("TRIER", t, t_cols), ("BGCARD", b, b_cols)]:
        if not df.empty:
            # CPF
            cpf_col = col_map.get('cpf', 'cpf')
            if cpf_col in df.columns:
                df['cpf'] = df[cpf_col].apply(clean_cpf)
            else:
                df['cpf'] = ""
            
            # Date
            data_col = col_map.get('data', 'data')
            if data_col in df.columns:
                df['data_emissao'] = df[data_col].apply(parse_date_br)
            else:
                df['data_emissao'] = None
            
            # Parcela
            parcela_col = col_map.get('parcela', 'parcela')
            if parcela_col in df.columns:
                df[['parcela_n', 'parcela_total']] = df[parcela_col].apply(
                    lambda x: pd.Series(parse_parcela(x))
                )
            else:
                df['parcela_n'] = None
                df['parcela_total'] = None
            
            # Valor Parcela
            valor_parcela_col = col_map.get('valor_parcela', 'valor_parcela')
            if valor_parcela_col in df.columns:
                df['valor_parcela_num'] = df[valor_parcela_col].apply(safe_float_convert)
            else:
                print(f"Warning: {df_name} missing 'valor_parcela' column, using 0")
                df['valor_parcela_num'] = 0.0
            
            # Valor Total
            valor_total_col = col_map.get('valor_total', 'valor_total')
            if valor_total_col in df.columns:
                df['valor_total_num'] = df[valor_total_col].apply(safe_float_convert)
            else:
                print(f"Warning: {df_name} missing 'valor_total' column, using 0")
                df['valor_total_num'] = 0.0
            
            # Cliente
            cliente_col = col_map.get('cliente', 'cliente')
            if cliente_col in df.columns:
                df['cliente_name'] = df[cliente_col].astype(str).str.strip()
            else:
                df['cliente_name'] = ""
            
            # Filial
            filial_col = col_map.get('filial', 'filial')
            if filial_col in df.columns:
                df['filial'] = df[filial_col].astype(str).str.strip()
            else:
                df['filial'] = ""

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
                if not row_t.get('cpf') or not row_b.get('cpf'):
                    continue

                # CPF must match
                if str(row_t['cpf']) != str(row_b['cpf']):
                    continue

                # parcela number must match (if both have it)
                if row_t.get('parcela_n') and row_b.get('parcela_n'):
                    try:
                        if int(row_t['parcela_n']) != int(row_b['parcela_n']):
                            continue
                    except (ValueError, TypeError):
                        pass

                # compare values with tolerance
                val_t = row_t.get('valor_parcela_num', 0)
                val_b = row_b.get('valor_parcela_num', 0)
                
                diff = abs(val_t - val_b)
                
                if diff <= VALUE_TOL:
                    # Check total as well
                    total_t = row_t.get('valor_total_num', 0)
                    total_b = row_b.get('valor_total_num', 0)
                    total_diff = abs(total_t - total_b)
                    
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
                
                trier_name = f"{trier_cliente} — PARCELA {trier_parcela}" if trier_cliente else f"PARCELA {trier_parcela}"
                bg_name = f"{bg_cliente} — PARCELA {bg_parcela}" if bg_cliente else f"PARCELA {bg_parcela}"

                # check total parcelas
                if (row_t.get('parcela_total') and row_b.get('parcela_total') and
                    str(row_t['parcela_total']) != str(row_b['parcela_total'])):
                    status = "⚠️ NUM DE PARCELAS DIVERGENTES"
                else:
                    status = "✅ OK"

                out.append([
                    format_value_for_json(row_t.get('filial', '')),
                    row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                    trier_name,
                    format_value_for_json(row_t.get('valor_parcela_num', 0)),
                    format_value_for_json(row_t.get('valor_total_num', 0)),
                    bg_name,
                    format_value_for_json(row_b.get('valor_parcela_num', 0)),
                    format_value_for_json(row_b.get('valor_total_num', 0)),
                    status
                ])

            # ---------- ONLY TRIER ----------
            else:
                trier_cliente = str(row_t.get('cliente_name', '')).strip()
                trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
                trier_name = f"{trier_cliente} — PARCELA {trier_parcela}" if trier_cliente else f"PARCELA {trier_parcela}"

                out.append([
                    format_value_for_json(row_t.get('filial', '')),
                    row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                    trier_name,
                    format_value_for_json(row_t.get('valor_parcela_num', 0)),
                    format_value_for_json(row_t.get('valor_total_num', 0)),
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
            bg_name = f"{bg_cliente} — PARCELA {bg_parcela}" if bg_cliente else f"PARCELA {bg_parcela}"

            out.append([
                format_value_for_json(row_b.get('filial', '')),
                row_b['data_emissao'].strftime("%d/%m/%Y") if row_b.get('data_emissao') else '',
                "-",
                "-",
                "-",
                bg_name,
                format_value_for_json(row_b.get('valor_parcela_num', 0)),
                format_value_for_json(row_b.get('valor_total_num', 0)),
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

        # Clear existing content
        ws_out.clear()
        
        # Prepare values with header
        values = [HEADER] + rows
        
        # Update using correct parameter order (values first, then range)
        ws_out.update(values=values, range_name='A1')
        
        print(f"Successfully updated {SHEET_OUT} with {len(rows)} rows")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
