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

SHEET_CREDCOMMERCE = "dados_cred_commerce"
SHEET_TRIER = "dados_trier"
SHEET_OUT = "TRIERxCREDCOM"

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

VALUE_TOL = 0.75  # tolerância para diferença de valores
DATE_TOL_DAYS = 5  # tolerância de dias para considerar mesma compra

# Color mapping for STATUS
COLOR_MAP = {
    "✅ OK": {"red": 0.8, "green": 0.9, "blue": 0.8},  # Light green
    "⚠️ NUM DE PARCELAS DIVERGENTES": {"red": 1.0, "green": 0.8, "blue": 0.8},  # Light red
    "⚠️ SOMENTE CREDCOMMERCE": {"red": 0.9, "green": 0.9, "blue": 1.0},  # Light blue
    "⚠️ SOMENTE TRIER": {"red": 1.0, "green": 0.95, "blue": 0.8},  # Light yellow
    "⚠️ VALORES DIVERGENTES": {"red": 1.0, "green": 0.7, "blue": 0.7}  # Darker red for value mismatch
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
    """Normalize DataFrame columns."""
    if df.empty:
        return df
    
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df


def parse_date_br(x):
    """Parse Brazilian date format."""
    if not x or pd.isna(x) or x == "":
        return None
    try:
        if isinstance(x, str):
            for fmt in ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d']:
                try:
                    d = datetime.strptime(x.strip(), fmt)
                    return d.date()
                except ValueError:
                    continue
        d = pd.to_datetime(x, dayfirst=True, errors="coerce")
        return None if pd.isna(d) else d.date()
    except:
        return None


def parse_parcela_cred(x):
    """
    Parse Credcommerce parcel format: just the number "1" -> (1, None)
    """
    if not x or pd.isna(x) or x == "":
        return (None, None)
    
    try:
        return (int(str(x).strip()), None)
    except:
        return (None, None)


def parse_parcela_trier(x):
    """
    Parse TRIER parcel format: "PARCELA 1/5" -> (1, 5)
    """
    if not x or pd.isna(x) or x == "":
        return (None, None)
    
    x = str(x).replace("PARCELA", "").strip()
    m = re.search(r"(\d+)\s*[\/\-]\s*(\d+)", str(x))
    if not m:
        return (None, None)
    
    return int(m.group(1)), int(m.group(2))


def safe_float_convert(value):
    """Safely convert value to float, handling currency formats."""
    if pd.isna(value) or value == "" or value is None:
        return 0.0
    
    try:
        if isinstance(value, (int, float)):
            return float(value)
        
        s = str(value).strip()
        s = re.sub(r'[R$\s]', '', s)
        s = s.replace('.', '')
        s = s.replace(',', '.')
        
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
    """Format value to be JSON serializable."""
    if pd.isna(val) or val is None or val == "":
        return ""
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return 0.0
    if isinstance(val, (int, float)):
        return round(float(val), 2)
    return str(val)


def format_brl(v):
    """Format value as Brazilian currency."""
    if v is None or pd.isna(v) or v == "" or v == 0:
        return "-"
    try:
        s = f"{float(v):,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except:
        return "-"


def read_existing_annotations(ws_out) -> dict:
    """Read existing annotations from output sheet."""
    try:
        values = ws_out.get_all_values()
    except:
        return {}
        
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (8 - len(row))
        
        # Use composite key based on Filial, Data, and Valor to identify the row
        filial = row[0]
        data = row[1]
        cred_text = row[2] or ""
        trier_text = row[4] or ""
        
        # Extract parcela and valor from either side
        combined_text = cred_text + " " + trier_text
        parcela_match = re.search(r'PARCELA (\d+)/(\d+)', combined_text)
        if not parcela_match:
            parcela_match = re.search(r'— PARCELA (\d+)', combined_text)
        
        valor = row[3] if row[3] != "-" else row[5]
        
        if parcela_match:
            parcela_key = parcela_match.group(0)
            composite_key = f"{filial}|{data}|{parcela_key}|{valor}"
        else:
            composite_key = f"{filial}|{data}|{combined_text}|{valor}"

        # Get annotation from column H (index 7)
        anot = (row[7] or "").strip()
        
        if anot:
            annotations[composite_key] = anot

    return annotations


def apply_status_coloring(ws, num_rows: int):
    """Apply background color to STATUS column."""
    try:
        requests = []
        
        status_range = f"G2:G{num_rows + 1}"
        status_values = ws.get(status_range)
        
        for i, row in enumerate(status_values, start=2):
            if row and row[0] in COLOR_MAP:
                color = COLOR_MAP[row[0]]
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": i - 1,
                            "endRowIndex": i,
                            "startColumnIndex": 6,
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
            for i in range(0, len(requests), 100):
                chunk = requests[i:i + 100]
                ws.spreadsheet.batch_update({"requests": chunk})
                
    except Exception as e:
        print(f"Note: Could not apply status coloring: {e}")


def group_purchases_by_value_and_parcelas(df, source='cred'):
    """
    Group rows into purchases based on Filial, approximate date, and value pattern.
    Returns a dictionary with purchase keys and lists of indices.
    """
    purchases = {}
    
    for idx, row in df.iterrows():
        filial = row.get('filial', '')
        data = row.get('data_emissao')
        valor = row.get('valor_parcela_num', 0)
        
        if source == 'cred':
            parcela_n = row.get('parcela_n_cred')
            # For Credcommerce, we don't know total parcels from a single row
            # We'll need to group by filial, date, and value
        else:  # trier
            parcela_n = row.get('parcela_n_trier')
            parcela_total = row.get('parcela_total_trier')
        
        if not filial or not data or not valor:
            continue
        
        # Create a key based on filial and rounded value
        valor_rounded = round(valor, 2)
        
        # Look for matching purchase group
        found_group = False
        for purchase_key in list(purchases.keys()):
            p_filial, p_data_str, p_valor = purchase_key.split('|')
            p_valor = float(p_valor)
            p_data = datetime.strptime(p_data_str, '%Y-%m-%d').date()
            
            if (filial == p_filial and 
                abs(valor_rounded - p_valor) <= VALUE_TOL and
                abs((data - p_data).days) <= DATE_TOL_DAYS):
                
                purchases[purchase_key].append(idx)
                found_group = True
                break
        
        if not found_group:
            purchase_key = f"{filial}|{data.isoformat()}|{valor_rounded}"
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
    
    # Normalize DataFrames
    c = normalize_df_columns(df_cred)
    t = normalize_df_columns(df_trier)
    
    print(f"CREDCOMMERCE columns: {list(c.columns)}")
    print(f"TRIER columns: {list(t.columns)}")
    
    # Process CREDCOMMERCE data
    if not c.empty:
        c['filial'] = c['filial'].astype(str).str.strip()
        c['cliente_name'] = c['cliente'].astype(str).str.strip()
        c['data_emissao'] = c['data emissao'].apply(parse_date_br)
        c['valor_parcela_num'] = c['valor'].apply(safe_float_convert)
        
        # Parse parcela for Credcommerce (just the number)
        c[['parcela_n_cred', 'parcela_total_cred']] = c['parcela'].apply(
            lambda x: pd.Series(parse_parcela_cred(x))
        )
        
        # Drop rows with missing essential data
        c = c.dropna(subset=['filial', 'data_emissao', 'parcela_n_cred'])
    
    # Process TRIER data
    if not t.empty:
        t['filial'] = t['filial'].astype(str).str.strip()
        t['cliente_name'] = t['cliente'].astype(str).str.strip()
        t['data_emissao'] = t['data emissao'].apply(parse_date_br)
        t['valor_parcela_num'] = t['valor'].apply(safe_float_convert)
        
        # Parse parcela for TRIER (with total)
        t[['parcela_n_trier', 'parcela_total_trier']] = t['parcela'].apply(
            lambda x: pd.Series(parse_parcela_trier(x))
        )
        
        # Drop rows with missing essential data
        t = t.dropna(subset=['filial', 'data_emissao', 'parcela_n_trier', 'parcela_total_trier'])
    
    # Group purchases
    cred_purchases = group_purchases_by_value_and_parcelas(c, 'cred') if not c.empty else {}
    trier_purchases = group_purchases_by_value_and_parcelas(t, 'trier') if not t.empty else {}
    
    # Track used rows
    used_cred_indices = set()
    used_trier_indices = set()
    out = []
    
    # First, match purchases based on filial, date, and value
    for cred_key, cred_indices in cred_purchases.items():
        cred_filial, cred_data_str, cred_valor = cred_key.split('|')
        cred_data = datetime.strptime(cred_data_str, '%Y-%m-%d').date()
        cred_valor = float(cred_valor)
        
        # Find matching TRIER purchase
        best_match_key = None
        best_match_diff = float('inf')
        
        for trier_key, trier_indices in trier_purchases.items():
            if all(idx in used_trier_indices for idx in trier_indices):
                continue
                
            t_filial, t_data_str, t_valor = trier_key.split('|')
            t_data = datetime.strptime(t_data_str, '%Y-%m-%d').date()
            t_valor = float(t_valor)
            
            # Check filial match
            if cred_filial != t_filial:
                continue
            
            # Check date within tolerance
            if abs((cred_data - t_data).days) > DATE_TOL_DAYS:
                continue
            
            # Check value within tolerance
            if abs(cred_valor - t_valor) <= VALUE_TOL:
                # This is a potential match
                if abs(cred_valor - t_valor) < best_match_diff:
                    best_match_diff = abs(cred_valor - t_valor)
                    best_match_key = trier_key
        
        if best_match_key:
            # Found a matching purchase
            trier_indices = trier_purchases[best_match_key]
            
            # Create maps for quick lookup
            cred_parcela_map = {c.loc[idx, 'parcela_n_cred']: idx for idx in cred_indices}
            trier_parcela_map = {t.loc[idx, 'parcela_n_trier']: idx for idx in trier_indices}
            
            # Check if number of parcels match
            if len(cred_indices) == len(trier_indices):
                status_base = "✅ OK"
            else:
                status_base = "⚠️ NUM DE PARCELAS DIVERGENTES"
            
            # Match each CREDCOMMERCE row with corresponding TRIER row
            for cred_idx in cred_indices:
                row_c = c.loc[cred_idx]
                cred_parcela_n = row_c['parcela_n_cred']
                
                # Find matching TRIER row with same parcela number
                if cred_parcela_n in trier_parcela_map:
                    trier_idx = trier_parcela_map[cred_parcela_n]
                    row_t = t.loc[trier_idx]
                    
                    # Mark as used
                    used_cred_indices.add(cred_idx)
                    used_trier_indices.add(trier_idx)
                    
                    # Format names
                    cred_name = f"{row_c['cliente_name']} — PARCELA {cred_parcela_n}"
                    trier_name = f"{row_t['cliente_name']} — PARCELA {row_t['parcela_n_trier']}/{row_t['parcela_total_trier']}"
                    
                    out.append([
                        row_c['filial'],
                        row_c['data_emissao'].strftime("%d/%m/%Y"),
                        cred_name,
                        format_brl(row_c['valor_parcela_num']),
                        trier_name,
                        format_brl(row_t['valor_parcela_num']),
                        status_base,
                        ""  # Placeholder for annotations
                    ])
            
            # Add any unmatched TRIER rows from this purchase
            for trier_idx in trier_indices:
                if trier_idx not in used_trier_indices:
                    row_t = t.loc[trier_idx]
                    
                    trier_name = f"{row_t['cliente_name']} — PARCELA {row_t['parcela_n_trier']}/{row_t['parcela_total_trier']}"
                    
                    out.append([
                        row_t['filial'],
                        row_t['data_emissao'].strftime("%d/%m/%Y"),
                        "-",
                        "-",
                        trier_name,
                        format_brl(row_t['valor_parcela_num']),
                        "⚠️ SOMENTE TRIER",
                        ""
                    ])
                    used_trier_indices.add(trier_idx)
        else:
            # No matching purchase found - all CREDCOMMERCE rows are "SOMENTE CREDCOMMERCE"
            for cred_idx in cred_indices:
                row_c = c.loc[cred_idx]
                
                cred_name = f"{row_c['cliente_name']} — PARCELA {row_c['parcela_n_cred']}"
                
                out.append([
                    row_c['filial'],
                    row_c['data_emissao'].strftime("%d/%m/%Y"),
                    cred_name,
                    format_brl(row_c['valor_parcela_num']),
                    "-",
                    "-",
                    "⚠️ SOMENTE CREDCOMMERCE",
                    ""
                ])
                used_cred_indices.add(cred_idx)
    
    # Add remaining TRIER purchases (SOMENTE TRIER)
    for trier_key, trier_indices in trier_purchases.items():
        for trier_idx in trier_indices:
            if trier_idx not in used_trier_indices:
                row_t = t.loc[trier_idx]
                
                trier_name = f"{row_t['cliente_name']} — PARCELA {row_t['parcela_n_trier']}/{row_t['parcela_total_trier']}"
                
                out.append([
                    row_t['filial'],
                    row_t['data_emissao'].strftime("%d/%m/%Y"),
                    "-",
                    "-",
                    trier_name,
                    format_brl(row_t['valor_parcela_num']),
                    "⚠️ SOMENTE TRIER",
                    ""
                ])
    
    # Sort by Filial and Date
    out.sort(key=lambda x: (x[0], x[1]))
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
            annotations = read_existing_annotations(ws_out)
            ws_out.clear()
        except gspread.WorksheetNotFound:
            ws_out = sh.add_worksheet(title=SHEET_OUT, rows=2000, cols=8)
            annotations = {}
            print(f"Created new worksheet: {SHEET_OUT}")

        # Apply annotations to rows
        for row in rows:
            # Create composite key for annotation lookup
            filial = row[0]
            data = row[1]
            cred_text = row[2] if row[2] != "-" else ""
            trier_text = row[4] if row[4] != "-" else ""
            valor = row[3] if row[3] != "-" else row[5]
            
            combined_text = cred_text + " " + trier_text
            parcela_match = re.search(r'PARCELA (\d+)/(\d+)', combined_text)
            if not parcela_match:
                parcela_match = re.search(r'— PARCELA (\d+)', combined_text)
            
            if parcela_match:
                parcela_key = parcela_match.group(0)
                composite_key = f"{filial}|{data}|{parcela_key}|{valor}"
            else:
                composite_key = f"{filial}|{data}|{combined_text}|{valor}"
            
            if composite_key in annotations:
                row[7] = annotations[composite_key]
        
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
