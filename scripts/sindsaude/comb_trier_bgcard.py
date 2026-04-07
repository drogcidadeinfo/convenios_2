import re
import os
import json
import unicodedata
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Config
# ----------------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

SHEET_TRIER = "dados_trier_sind"
SHEET_BGCARD = "dados_bgcard"
SHEET_OUT = "TRIERxSIND"

# Updated header
HEADER = [
    "Filial",
    "Data Emissão",
    "TRIER",
    "Valor Parcela",
    "Valor Total",
    "BGCARD",
    "Valor Parcela",
    "Valor Total",
    "STATUS",
    "Anotações"
]

VALUE_TOL = 0.75   # tolerância para diferença de valores
DATE_TOL_DAYS = 5  # tolerância de dias para considerar mesma compra

# Color mapping for STATUS
COLOR_MAP = {
    "✅ OK": {"red": 0.8, "green": 0.9, "blue": 0.8},  # Light green
    "⚠️ NUM DE PARCELAS DIVERGENTES": {"red": 1.0, "green": 0.8, "blue": 0.8},  # Light red
    "⚠️ SOMENTE TRIER": {"red": 1.0, "green": 0.95, "blue": 0.8},  # Light yellow
    "⚠️ SOMENTE BGCARD": {"red": 0.9, "green": 0.9, "blue": 1.0},  # Light blue
    "⚠️ VALORES DIVERGENTES": {"red": 1.0, "green": 0.7, "blue": 0.7}  # Darker red for value mismatch
}

# Column mappings (original -> normalized)
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

def normalize_money_key(v):
    """Convert BRL formatted string to normalized float string for key usage."""
    if not v or v == "-":
        return "0.00"
    
    v = str(v)
    v = re.sub(r"[^\d,.-]", "", v)  # remove R$, spaces, etc
    v = v.replace(".", "").replace(",", ".")
    
    try:
        return f"{float(v):.2f}"
    except:
        return "0.00"

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


def format_brl(v):
    """Format value as Brazilian currency."""
    if v is None or pd.isna(v) or v == "":
        return "-"
    try:
        s = f"{float(v):,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except:
        return "-"


def normalize_cpf(x):
    """Alias for clean_cpf for compatibility."""
    return clean_cpf(x)


def read_existing_annotations(ws_out) -> dict:
    """
    Read only the Anotações column, keyed by a composite key (CPF + Parcela + Valor Total)
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (10 - len(row))

        # Extract CPF and parcela from the text
        trier_text = row[2] or ""
        bg_text = row[5] or ""
        
        # Extract CPF
        cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', trier_text + " " + bg_text)
        if not cpf_match:
            continue
            
        cpf_digits = re.sub(r"\D", "", cpf_match.group(1))
        
        # Extract parcela info
        parcela_match = re.search(r'PARCELA (\d+)/(\d+)', trier_text + " " + bg_text)
        if not parcela_match:
            continue
            
        parcela_num = parcela_match.group(1)
        parcela_total = parcela_match.group(2)
        parcela_key = f"{parcela_num}/{parcela_total}"
        
        # Get valor total to help identify the purchase
        # valor_total = row[4] if row[4] != "-" else row[7]  # TRIER total or BGCARD total
        valor_total_raw = row[4] if row[4] != "-" else row[7]
        valor_total = normalize_money_key(valor_total_raw)

        # Create composite key
        composite_key = f"{cpf_digits}|{parcela_key}|{valor_total}"

        # Get annotation from column J (index 9)
        anot = (row[9] or "").strip()
        
        # Store by composite key
        if composite_key not in annotations and anot:
            annotations[composite_key] = anot

    return annotations


def apply_status_coloring(ws, num_rows: int):
    """
    Apply background color to STATUS column based on the status value
    """
    try:
        requests = []
        
        # Get all status values to determine coloring
        status_range = f"I2:I{num_rows + 1}"  # STATUS is now column I
        status_values = ws.get(status_range)
        
        for i, row in enumerate(status_values, start=2):  # start from row 2 (after header)
            if row and row[0] in COLOR_MAP:
                color = COLOR_MAP[row[0]]
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": i - 1,
                            "endRowIndex": i,
                            "startColumnIndex": 8,  # Column I (0-based)
                            "endColumnIndex": 9
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": color
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
        
        if requests:
            # Batch update in chunks of 100 to avoid quota issues
            for i in range(0, len(requests), 100):
                chunk = requests[i:i + 100]
                ws.spreadsheet.batch_update({"requests": chunk})
                
    except Exception as e:
        print(f"Note: Could not apply status coloring: {e}")


def group_parcels_by_purchase(df):
    """
    Group TRIER rows by purchase (CPF + Valor Total + approximate date)
    Returns a dictionary with purchase keys and lists of parcel indices
    """
    purchases = {}
    
    for idx, row in df.iterrows():
        cpf = row.get('cpf', '')
        valor_total = row.get('valor_total_num', 0)
        data = row.get('data_emissao')
        
        if not cpf or not data:
            continue
            
        # Round valor_total to 2 decimals to handle small differences
        valor_total_rounded = round(valor_total, 2)
        
        # Find matching purchase group
        found_group = False
        for purchase_key in purchases.keys():
            p_cpf, p_valor, p_data = purchase_key.split('|')
            p_valor = float(p_valor)
            p_data = datetime.strptime(p_data, '%Y-%m-%d').date()
            
            if (cpf == p_cpf and 
                abs(valor_total_rounded - p_valor) <= VALUE_TOL and
                abs((data - p_data).days) <= DATE_TOL_DAYS):
                purchases[purchase_key].append(idx)
                found_group = True
                break
        
        if not found_group:
            # Create new purchase group
            purchase_key = f"{cpf}|{valor_total_rounded}|{data.isoformat()}"
            purchases[purchase_key] = [idx]
    
    return purchases


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
                # Clean up client name (remove extra spaces, normalize)
                df['cliente_name'] = df['cliente_name'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
            else:
                df['cliente_name'] = ""
            
            # Filial
            filial_col = col_map.get('filial', 'filial')
            if filial_col in df.columns:
                df['filial'] = df[filial_col].astype(str).str.strip()
            else:
                df['filial'] = ""

    # Group TRIER rows by purchase
    trier_purchases = group_parcels_by_purchase(t) if not t.empty else {}
    
    # Track used purchases
    used_trier_purchases = set()
    used_bg_rows = set()
    out = []
    
    # Process each BGCARD row and find matching TRIER purchase
    if not b.empty:
        for j, row_b in b.iterrows():
            bg_cpf = row_b.get('cpf', '')
            bg_valor_total = row_b.get('valor_total_num', 0)
            bg_data = row_b.get('data_emissao')
            bg_parcela_n = row_b.get('parcela_n')
            bg_parcela_total = row_b.get('parcela_total')
            
            if not bg_cpf or not bg_data:
                # Skip rows without required data
                continue
                
            # Find matching TRIER purchase
            matching_purchase_key = None
            best_match_score = float('inf')
            
            for purchase_key, trier_indices in trier_purchases.items():
                if purchase_key in used_trier_purchases:
                    continue
                    
                p_cpf, p_valor, p_data = purchase_key.split('|')
                p_valor = float(p_valor)
                p_data = datetime.strptime(p_data, '%Y-%m-%d').date()
                
                # Check if CPF matches
                if bg_cpf != p_cpf:
                    continue
                
                # Check if valor total matches within tolerance
                valor_diff = abs(bg_valor_total - p_valor)
                if valor_diff > VALUE_TOL:
                    continue
                
                # Check if date is within tolerance
                date_diff = abs((bg_data - p_data).days)
                if date_diff > DATE_TOL_DAYS:
                    continue
                
                # This is a potential match
                if valor_diff < best_match_score:
                    best_match_score = valor_diff
                    matching_purchase_key = purchase_key
            
            if matching_purchase_key:
                # Found a matching purchase
                used_trier_purchases.add(matching_purchase_key)
                used_bg_rows.add(j)
                
                trier_indices = trier_purchases[matching_purchase_key]
                
                # Find the TRIER row with matching parcela number
                matching_trier_idx = None
                for idx in trier_indices:
                    row_t = t.loc[idx]
                    if row_t.get('parcela_n') == bg_parcela_n:
                        matching_trier_idx = idx
                        break
                
                # If no exact parcela match, use the first one
                if matching_trier_idx is None and trier_indices:
                    matching_trier_idx = trier_indices[0]
                
                if matching_trier_idx is not None:
                    row_t = t.loc[matching_trier_idx]
                    
                    # Format names
                    trier_cliente = str(row_t.get('cliente_name', '')).strip()
                    bg_cliente = str(row_b.get('cliente_name', '')).strip()
                    
                    trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
                    bg_parcela = f"{bg_parcela_n}/{bg_parcela_total}" if bg_parcela_n and bg_parcela_total else "?/?"
                    
                    trier_name = f"{trier_cliente} — PARCELA {trier_parcela}"
                    bg_name = f"{bg_cliente} — PARCELA {bg_parcela}"
                    
                    # Check if total parcelas match
                    if row_t.get('parcela_total') == bg_parcela_total:
                        status = "✅ OK"
                    else:
                        status = "⚠️ NUM DE PARCELAS DIVERGENTES"
                    
                    out.append([
                        format_value_for_json(row_t.get('filial', '')),
                        row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                        trier_name,
                        format_brl(row_t.get('valor_parcela_num', 0)),
                        format_brl(row_t.get('valor_total_num', 0)),
                        bg_name,
                        format_brl(row_b.get('valor_parcela_num', 0)),
                        format_brl(row_b.get('valor_total_num', 0)),
                        status,
                        ""  # Placeholder for annotations
                    ])
            else:
                # No matching purchase found in TRIER
                bg_cliente = str(row_b.get('cliente_name', '')).strip()
                bg_parcela = f"{bg_parcela_n}/{bg_parcela_total}" if bg_parcela_n and bg_parcela_total else "?/?"
                bg_name = f"{bg_cliente} — PARCELA {bg_parcela}"
                
                out.append([
                    format_value_for_json(row_b.get('filial', '')),
                    row_b['data_emissao'].strftime("%d/%m/%Y") if row_b.get('data_emissao') else '',
                    "-",
                    "-",
                    "-",
                    bg_name,
                    format_brl(row_b.get('valor_parcela_num', 0)),
                    format_brl(row_b.get('valor_total_num', 0)),
                    "⚠️ SOMENTE BGCARD",
                    ""  # Placeholder for annotations
                ])
    
    # Process remaining TRIER purchases (SOMENTE TRIER)
    for purchase_key, trier_indices in trier_purchases.items():
        if purchase_key in used_trier_purchases:
            continue
            
        # For each purchase, show the first parcel (usually parcela 1)
        # Sort indices by parcela number to find the first one
        sorted_indices = sorted(trier_indices, 
                              key=lambda idx: (t.loc[idx].get('parcela_n', float('inf'))))
        
        if sorted_indices:
            # Show the first parcel of this purchase
            idx = sorted_indices[0]
            row_t = t.loc[idx]
            
            trier_cliente = str(row_t.get('cliente_name', '')).strip()
            trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
            trier_name = f"{trier_cliente} — PARCELA {trier_parcela}"
            
            out.append([
                format_value_for_json(row_t.get('filial', '')),
                row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                trier_name,
                format_brl(row_t.get('valor_parcela_num', 0)),
                format_brl(row_t.get('valor_total_num', 0)),
                "-",
                "-",
                "-",
                "⚠️ SOMENTE TRIER",
                ""  # Placeholder for annotations
            ])

    # Sort by Filial then CPF (extracted from name)
    def sort_key(row):
        filial = row[0]
        # Extract CPF from TRIER or BGCARD name
        name = row[2] if row[2] != "-" else row[5]
        cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', name)
        cpf = cpf_match.group(1) if cpf_match else ""
        return (filial, cpf)
    
    out.sort(key=sort_key)
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
            # Read existing annotations before clearing
            annotations = read_existing_annotations(ws_out)
            ws_out.clear()
        except gspread.WorksheetNotFound:
            ws_out = sh.add_worksheet(title=SHEET_OUT, rows=2000, cols=10)
            annotations = {}
            print(f"Created new worksheet: {SHEET_OUT}")

        # Apply annotations to rows
        for row in rows:
            # Create composite key for annotation lookup
            trier_text = row[2] if row[2] != "-" else ""
            bg_text = row[5] if row[5] != "-" else ""
            
            # Extract CPF and parcela
            cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', trier_text + " " + bg_text)
            parcela_match = re.search(r'PARCELA (\d+)/(\d+)', trier_text + " " + bg_text)
            # valor_total = row[4] if row[4] != "-" else row[7]
            valor_total_raw = row[4] if row[4] != "-" else row[7]
            valor_total = normalize_money_key(valor_total_raw)
            
            if cpf_match and parcela_match:
                cpf_digits = re.sub(r"\D", "", cpf_match.group(1))
                parcela_key = parcela_match.group(0).replace("PARCELA ", "")
                composite_key = f"{cpf_digits}|{parcela_key}|{valor_total}"
                
                # Apply annotation if exists
                if composite_key in annotations:
                    row[9] = annotations[composite_key]  # Anotações column
        
        # Prepare values with header
        values = [HEADER] + rows
        
        # Update using correct parameter order (values first, then range)
        ws_out.update(values=values, range_name='A1')
        
        # Apply status coloring
        if rows:
            apply_status_coloring(ws_out, len(rows))
        
        print(f"Successfully updated {SHEET_OUT} with {len(rows)} rows")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
