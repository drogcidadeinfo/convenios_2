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

SHEET_CREDCOMMERCE = "credcommerse"  # Note: typo in your sheet name
SHEET_TRIER = "dados_trier_sind"
SHEET_OUT = "TRIERxSIND"

HEADER = [
    "Filial",
    "Data Emissão",
    "CREDCOMMERCE",
    "Valor Parcela",
    "TRIER",
    "Valor Parcela",
    "STATUS",
    "Anotações"
]

VALUE_TOL = 0.75   # tolerância para diferença de valores
TRIER_DISCOUNT = 0.95  # 5% desconto (TRIER já vem com desconto, Credcommerce sem)
DATE_TOL_DAYS = 5  # tolerância de dias para considerar mesma compra

# Color mapping for STATUS
COLOR_MAP = {
    "✅ OK": {"red": 0.8, "green": 0.9, "blue": 0.8},  # Light green
    "⚠️ NUM DE PARCELAS DIVERGENTES": {"red": 1.0, "green": 0.8, "blue": 0.8},  # Light red
    "⚠️ SOMENTE CREDCOMMERCE": {"red": 0.9, "green": 0.9, "blue": 1.0},  # Light blue
    "⚠️ SOMENTE TRIER": {"red": 1.0, "green": 0.95, "blue": 0.8},  # Light yellow
    "⚠️ VALORES DIVERGENTES": {"red": 1.0, "green": 0.7, "blue": 0.7}  # Darker red for value mismatch
}

# Column mappings
COLUMN_MAPPING = {
    'filial': 'filial',
    'cliente': 'cliente',
    'data emissão': 'data',
    'data_emissao': 'data',
    'data': 'data',
    'parcela': 'parcela',
    'valor': 'valor_parcela',
    'valor parcela': 'valor_parcela',
    'valor_parcela': 'valor_parcela'
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
    Parse parcel format like "1/5" or "PARCELA 1/5" -> (1, 5)
    """
    if not x or pd.isna(x) or x == "":
        return (None, None)

    # Handle "PARCELA 1/5" format
    x = str(x).replace("PARCELA", "").strip()
    
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


def read_existing_annotations(ws_out) -> dict:
    """
    Read only the Anotações column, keyed by a composite key (CPF + Parcela + Valor)
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (8 - len(row))

        # Extract CPF and parcela from the text
        cred_text = row[2] or ""
        trier_text = row[4] or ""
        
        # Extract CPF from either column
        combined_text = cred_text + " " + trier_text
        cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', combined_text)
        if not cpf_match:
            continue
            
        cpf_digits = re.sub(r"\D", "", cpf_match.group(1))
        
        # Extract parcela info
        parcela_match = re.search(r'PARCELA (\d+)/(\d+)', combined_text)
        if not parcela_match:
            continue
            
        parcela_num = parcela_match.group(1)
        parcela_total = parcela_match.group(2)
        parcela_key = f"{parcela_num}/{parcela_total}"
        
        # Get valor to help identify the purchase
        valor = row[3] if row[3] != "-" else row[5]  # CREDCOMMERCE valor or TRIER valor

        # Create composite key
        composite_key = f"{cpf_digits}|{parcela_key}|{valor}"

        # Get annotation from column H (index 7)
        anot = (row[7] or "").strip()
        
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
        status_range = f"G2:G{num_rows + 1}"  # STATUS is column G
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
                            "startColumnIndex": 6,  # Column G (0-based)
                            "endColumnIndex": 7
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


def group_parcels_by_purchase(df, source='trier'):
    """
    Group rows by purchase (CPF + approximate date + value pattern)
    For Credcommerce, we don't have valor total, so we group by valor parcela pattern
    """
    purchases = {}
    
    for idx, row in df.iterrows():
        cpf = row.get('cpf', '')
        valor_parcela = row.get('valor_parcela_num', 0)
        data = row.get('data_emissao')
        parcela_total = row.get('parcela_total')
        
        if not cpf or not data or not parcela_total:
            continue
            
        # Estimate total value based on parcel value and total parcels
        estimated_total = valor_parcela * parcela_total
        
        # Find matching purchase group
        found_group = False
        for purchase_key in purchases.keys():
            p_cpf, p_data, p_parcela_total, p_estimated = purchase_key.split('|')
            p_data = datetime.strptime(p_data, '%Y-%m-%d').date()
            p_estimated = float(p_estimated)
            
            if (cpf == p_cpf and 
                parcela_total == int(p_parcela_total) and
                abs(estimated_total - p_estimated) <= (VALUE_TOL * parcela_total) and
                abs((data - p_data).days) <= DATE_TOL_DAYS):
                
                purchases[purchase_key].append(idx)
                found_group = True
                break
        
        if not found_group:
            # Create new purchase group
            purchase_key = f"{cpf}|{data.isoformat()}|{parcela_total}|{estimated_total}"
            purchases[purchase_key] = [idx]
    
    return purchases


# ----------------------------
# Core logic
# ----------------------------
def build_rows(df_cred, df_trier):
    """Build comparison rows between CREDCOMMERCE and TRIER data."""
    
    # Handle empty DataFrames
    if df_cred.empty and df_trier.empty:
        return []
    
    # Normalize DataFrames and get column mappings
    c, c_cols = normalize_df_columns(df_cred)
    t, t_cols = normalize_df_columns(df_trier)
    
    print(f"CREDCOMMERCE columns: {list(c.columns)}")
    print(f"TRIER columns: {list(t.columns)}")
    
    # Standardize columns
    for df_name, df, col_map in [("CREDCOMMERCE", c, c_cols), ("TRIER", t, t_cols)]:
        if not df.empty:
            # Filial
            filial_col = col_map.get('filial', 'filial')
            if filial_col in df.columns:
                df['filial'] = df[filial_col].astype(str).str.strip()
            else:
                df['filial'] = ""
            
            # Cliente
            cliente_col = col_map.get('cliente', 'cliente')
            if cliente_col in df.columns:
                df['cliente_name'] = df[cliente_col].astype(str).str.strip()
                df['cliente_name'] = df['cliente_name'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
            else:
                df['cliente_name'] = ""
            
            # Data
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
            valor_col = col_map.get('valor_parcela', 'valor_parcela')
            if valor_col in df.columns:
                df['valor_parcela_num'] = df[valor_col].apply(safe_float_convert)
            else:
                print(f"Warning: {df_name} missing 'valor_parcela' column, using 0")
                df['valor_parcela_num'] = 0.0
            
            # CPF - Try to extract from cliente name if not present
            if 'cpf' not in df.columns:
                # Try to extract CPF from cliente column
                df['cpf'] = df['cliente_name'].apply(lambda x: clean_cpf(x) if x else "")
            else:
                df['cpf'] = df['cpf'].apply(clean_cpf)

    # Group TRIER rows by purchase
    trier_purchases = group_parcels_by_purchase(t, 'trier') if not t.empty else {}
    
    # Group CREDCOMMERCE rows by purchase
    cred_purchases = group_parcels_by_purchase(c, 'cred') if not c.empty else {}
    
    # Track used purchases
    used_trier_purchases = set()
    used_cred_purchases = set()
    out = []
    
    # Process each CREDCOMMERCE purchase and find matching TRIER purchase
    if not c.empty:
        for cred_key, cred_indices in cred_purchases.items():
            # Get first row of the purchase (all have same data)
            cred_idx = cred_indices[0]
            row_c = c.loc[cred_idx]
            
            cred_cpf = row_c.get('cpf', '')
            cred_parcela_total = row_c.get('parcela_total')
            cred_data = row_c.get('data_emissao')
            
            if not cred_cpf or not cred_data:
                continue
                
            # Find matching TRIER purchase
            matching_trier_key = None
            best_match_score = float('inf')
            
            for trier_key, trier_indices in trier_purchases.items():
                if trier_key in used_trier_purchases:
                    continue
                    
                t_idx = trier_indices[0]  # First row of the purchase
                row_t = t.loc[t_idx]
                
                t_cpf, t_data, t_parcela_total, t_estimated = trier_key.split('|')
                t_data = datetime.strptime(t_data, '%Y-%m-%d').date()
                
                # Check if CPF matches
                if cred_cpf != t_cpf:
                    continue
                
                # Check if total parcels match (approximate)
                if cred_parcela_total != int(t_parcela_total):
                    continue
                
                # Check if date is within tolerance
                date_diff = abs((cred_data - t_data).days)
                if date_diff > DATE_TOL_DAYS:
                    continue
                
                # Check values with discount
                # TRIER already has discount, CREDCOMMERCE doesn't
                # So we compare: TRIER value ≈ CREDCOMMERCE value * 0.95
                cred_valor = row_c.get('valor_parcela_num', 0)
                cred_valor_discounted = cred_valor * TRIER_DISCOUNT
                
                # Calculate average value difference across all parcels
                total_diff = 0
                for t_idx in trier_indices:
                    row_t = t.loc[t_idx]
                    t_valor = row_t.get('valor_parcela_num', 0)
                    diff = abs(t_valor - cred_valor_discounted)
                    total_diff += diff
                
                avg_diff = total_diff / len(trier_indices) if trier_indices else float('inf')
                
                if avg_diff < best_match_score:
                    best_match_score = avg_diff
                    matching_trier_key = trier_key
            
            if matching_trier_key and best_match_score <= VALUE_TOL:
                # Found a matching purchase
                used_cred_purchases.add(cred_key)
                used_trier_purchases.add(matching_trier_key)
                
                trier_indices = trier_purchases[matching_trier_key]
                
                # Create a map of parcela numbers to TRIER rows
                trier_parcela_map = {}
                for idx in trier_indices:
                    row_t = t.loc[idx]
                    parcela_n = row_t.get('parcela_n')
                    if parcela_n:
                        trier_parcela_map[parcela_n] = idx
                
                # For each CREDCOMMERCE row in this purchase, find matching TRIER row
                for cred_idx in cred_indices:
                    row_c = c.loc[cred_idx]
                    cred_parcela_n = row_c.get('parcela_n')
                    
                    # Find matching TRIER row with same parcela number
                    matching_trier_idx = trier_parcela_map.get(cred_parcela_n)
                    
                    if matching_trier_idx is not None:
                        row_t = t.loc[matching_trier_idx]
                        
                        # Format names
                        cred_cliente = str(row_c.get('cliente_name', '')).strip()
                        t_cliente = str(row_t.get('cliente_name', '')).strip()
                        
                        # Extract CPF for display (if present in name)
                        cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', cred_cliente + " " + t_cliente)
                        cpf_display = f" ({cpf_match.group(1)})" if cpf_match else ""
                        
                        cred_name = f"{cred_cliente} — PARCELA {cred_parcela_n}/{cred_parcela_total}{cpf_display}"
                        t_name = f"{t_cliente} — PARCELA {row_t.get('parcela_n')}/{row_t.get('parcela_total')}{cpf_display}"
                        
                        # Check if all parcels are present
                        if len(cred_indices) == len(trier_indices):
                            status = "✅ OK"
                        else:
                            status = "⚠️ NUM DE PARCELAS DIVERGENTES"
                        
                        out.append([
                            format_value_for_json(row_c.get('filial', '')),
                            row_c['data_emissao'].strftime("%d/%m/%Y") if row_c.get('data_emissao') else '',
                            cred_name,
                            format_brl(row_c.get('valor_parcela_num', 0)),
                            t_name,
                            format_brl(row_t.get('valor_parcela_num', 0)),
                            status,
                            ""  # Placeholder for annotations
                        ])
            else:
                # No matching purchase found in TRIER
                for cred_idx in cred_indices:
                    row_c = c.loc[cred_idx]
                    
                    cred_cliente = str(row_c.get('cliente_name', '')).strip()
                    cred_parcela_n = row_c.get('parcela_n')
                    cred_parcela_total = row_c.get('parcela_total')
                    
                    # Extract CPF for display
                    cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', cred_cliente)
                    cpf_display = f" ({cpf_match.group(1)})" if cpf_match else ""
                    
                    cred_name = f"{cred_cliente} — PARCELA {cred_parcela_n}/{cred_parcela_total}{cpf_display}"
                    
                    out.append([
                        format_value_for_json(row_c.get('filial', '')),
                        row_c['data_emissao'].strftime("%d/%m/%Y") if row_c.get('data_emissao') else '',
                        cred_name,
                        format_brl(row_c.get('valor_parcela_num', 0)),
                        "-",
                        "-",
                        "⚠️ SOMENTE CREDCOMMERCE",
                        ""  # Placeholder for annotations
                    ])
    
    # Process remaining TRIER purchases (SOMENTE TRIER)
    for trier_key, trier_indices in trier_purchases.items():
        if trier_key in used_trier_purchases:
            continue
            
        for idx in trier_indices:
            row_t = t.loc[idx]
            
            t_cliente = str(row_t.get('cliente_name', '')).strip()
            
            # Extract CPF for display
            cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', t_cliente)
            cpf_display = f" ({cpf_match.group(1)})" if cpf_match else ""
            
            t_name = f"{t_cliente} — PARCELA {row_t.get('parcela_n')}/{row_t.get('parcela_total')}{cpf_display}"
            
            out.append([
                format_value_for_json(row_t.get('filial', '')),
                row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                "-",
                "-",
                t_name,
                format_brl(row_t.get('valor_parcela_num', 0)),
                "⚠️ SOMENTE TRIER",
                ""  # Placeholder for annotations
            ])

    # Sort by Filial then date
    def sort_key(row):
        filial = row[0]
        data = row[1]
        return (filial, data)
    
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
        ws_cred = safe_get_worksheet(sh, SHEET_CREDCOMMERCE)
        ws_trier = safe_get_worksheet(sh, SHEET_TRIER)
        
        if not ws_cred or not ws_trier:
            print("Error: Required worksheets not found")
            return

        # Read data
        df_cred = pd.DataFrame(ws_cred.get_all_records())
        df_trier = pd.DataFrame(ws_trier.get_all_records())

        print(f"Read {len(df_cred)} rows from CREDCOMMERCE, {len(df_trier)} rows from TRIER")

        rows = build_rows(df_cred, df_trier)

        # Get or create output worksheet
        try:
            ws_out = sh.worksheet(SHEET_OUT)
            # Read existing annotations before clearing
            annotations = read_existing_annotations(ws_out)
            ws_out.clear()
        except gspread.WorksheetNotFound:
            ws_out = sh.add_worksheet(title=SHEET_OUT, rows=2000, cols=8)
            annotations = {}
            print(f"Created new worksheet: {SHEET_OUT}")

        # Apply annotations to rows
        for row in rows:
            # Create composite key for annotation lookup
            cred_text = row[2] if row[2] != "-" else ""
            trier_text = row[4] if row[4] != "-" else ""
            
            combined_text = cred_text + " " + trier_text
            
            # Extract CPF and parcela
            cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', combined_text)
            parcela_match = re.search(r'PARCELA (\d+)/(\d+)', combined_text)
            valor = row[3] if row[3] != "-" else row[5]
            
            if cpf_match and parcela_match:
                cpf_digits = re.sub(r"\D", "", cpf_match.group(1))
                parcela_key = parcela_match.group(0).replace("PARCELA ", "")
                composite_key = f"{cpf_digits}|{parcela_key}|{valor}"
                
                # Apply annotation if exists
                if composite_key in annotations:
                    row[7] = annotations[composite_key]  # Anotações column
        
        # Prepare values with header
        values = [HEADER] + rows
        
        # Update
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
