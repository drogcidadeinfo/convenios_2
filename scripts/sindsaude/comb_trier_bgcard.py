import re
import os
import json
import unicodedata
from datetime import datetime
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

# Updated header - added Anotações
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

VALUE_TOL = 0.75   # tolerância

# Color mapping for STATUS
COLOR_MAP = {
    "✅ OK": {"red": 0.8, "green": 0.9, "blue": 0.8},  # Light green
    "⚠️ NUM DE PARCELAS DIVERGENTES": {"red": 1.0, "green": 0.8, "blue": 0.8},  # Light red
    "⚠️ SOMENTE TRIER": {"red": 1.0, "green": 0.95, "blue": 0.8},  # Light yellow
    "⚠️ SOMENTE BGCARD": {"red": 0.9, "green": 0.9, "blue": 1.0},  # Light blue
    "⚠️ VALORES DIVERGENTES": {"red": 1.0, "green": 0.7, "blue": 0.7}  # Darker red for value mismatch
}

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


def normalize_cpf(x):
    """Alias for clean_cpf for compatibility."""
    return clean_cpf(x)


def read_existing_annotations(ws_out) -> dict:
    """
    Read only the Anotações column, keyed by a composite key (CPF + Parcela)
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (10 - len(row))

        # Extract CPF from the BGCARD or TRIER column (they contain CPF in parentheses)
        # Format is usually "Name — PARCELA X/Y (CPF)"
        trier_text = row[2] or ""
        bg_text = row[5] or ""
        
        # Try to extract CPF from either column
        cpf_match = re.search(r'\((\d{3}\.\d{3}\.\d{3}-\d{2})\)', trier_text + " " + bg_text)
        if not cpf_match:
            continue
            
        cpf_digits = re.sub(r"\D", "", cpf_match.group(1))
        
        # Extract parcela info
        parcela_match = re.search(r'PARCELA (\d+)/(\d+)', trier_text + " " + bg_text)
        parcela_key = parcela_match.group(0) if parcela_match else "UNKNOWN"
        
        # Create composite key
        composite_key = f"{cpf_digits}|{parcela_key}"

        # Get annotation from column J (index 9)
        anot = (row[9] or "").strip()
        
        # Store by composite key
        if composite_key not in annotations and anot:
            annotations[composite_key] = anot
        elif anot and composite_key in annotations and not annotations[composite_key]:
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
    
    # Create lookup dictionary for BGCARD rows by CPF for faster matching
    bg_by_cpf = {}
    if not b.empty:
        for j, row_b in b.iterrows():
            cpf = row_b.get('cpf', '')
            if cpf:
                if cpf not in bg_by_cpf:
                    bg_by_cpf[cpf] = []
                bg_by_cpf[cpf].append(j)

    # Process TRIER rows
    if not t.empty:
        for i, row_t in t.iterrows():
            trier_cpf = row_t.get('cpf', '')
            
            if not trier_cpf:
                # Skip rows without CPF
                continue
                
            match_index = None
            best_match = None
            best_diff = float('inf')
            parcela_divergent_match = None
            
            # Get potential BGCARD matches for this CPF
            potential_matches = bg_by_cpf.get(trier_cpf, [])
            
            for j in potential_matches:
                if j in used_bg:
                    continue
                    
                row_b = b.loc[j]
                
                # MATCHING LOGIC: Check all fields except Filial and Cliente
                
                # Check parcela number (must match exactly if both have it)
                parcela_match = True
                if row_t.get('parcela_n') and row_b.get('parcela_n'):
                    try:
                        if int(row_t['parcela_n']) != int(row_b['parcela_n']):
                            parcela_match = False
                    except (ValueError, TypeError):
                        parcela_match = False
                
                # Check valor parcela with tolerance
                val_t = row_t.get('valor_parcela_num', 0)
                val_b = row_b.get('valor_parcela_num', 0)
                valor_parcela_match = abs(val_t - val_b) <= VALUE_TOL
                
                # Check valor total with tolerance
                total_t = row_t.get('valor_total_num', 0)
                total_b = row_b.get('valor_total_num', 0)
                valor_total_match = abs(total_t - total_b) <= VALUE_TOL
                
                # Calculate overall match score (for best match tracking)
                total_diff = abs(total_t - total_b)
                parcela_diff = abs(val_t - val_b)
                combined_diff = total_diff + parcela_diff
                
                # Check for NUM DE PARCELAS DIVERGENTES case (CPF + Valor Total match, but parcela differs)
                if valor_total_match and not parcela_match and trier_cpf == row_b.get('cpf', ''):
                    if parcela_divergent_match is None or combined_diff < best_diff:
                        parcela_divergent_match = j
                        best_diff = combined_diff
                
                # Check for full match (all fields match)
                if parcela_match and valor_parcela_match and valor_total_match:
                    match_index = j
                    break
                elif combined_diff < best_diff:
                    best_diff = combined_diff
                    best_match = j
            
            # Priority: 1. Full match, 2. Parcela divergent match, 3. Best partial match
            final_match_index = None
            match_type = None
            
            if match_index is not None:
                final_match_index = match_index
                match_type = "full"
            elif parcela_divergent_match is not None:
                final_match_index = parcela_divergent_match
                match_type = "parcela_divergent"
            elif best_match is not None:
                final_match_index = best_match
                match_type = "partial"

            # ---------- MATCH FOUND ----------
            if final_match_index is not None:
                used_bg.add(final_match_index)
                row_b = b.loc[final_match_index]

                # Format names safely with CPF
                trier_cliente = str(row_t.get('cliente_name', '')).strip()
                bg_cliente = str(row_b.get('cliente_name', '')).strip()
                
                # Format CPF for display
                cpf_display = trier_cpf
                if len(cpf_display) == 11:
                    cpf_display = f"{cpf_display[:3]}.{cpf_display[3:6]}.{cpf_display[6:9]}-{cpf_display[9:]}"
                
                trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
                bg_parcela = f"{row_b.get('parcela_n', '?')}/{row_b.get('parcela_total', '?')}"
                
                trier_name = f"{trier_cliente} — PARCELA {trier_parcela} ({cpf_display})" if trier_cliente else f"PARCELA {trier_parcela} ({cpf_display})"
                bg_name = f"{bg_cliente} — PARCELA {bg_parcela} ({cpf_display})" if bg_cliente else f"PARCELA {bg_parcela} ({cpf_display})"

                # Determine status based on match type
                if match_type == "full":
                    status = "✅ OK"
                elif match_type == "parcela_divergent":
                    status = "⚠️ NUM DE PARCELAS DIVERGENTES"
                else:
                    # Check if it's value mismatch
                    val_t = row_t.get('valor_parcela_num', 0)
                    val_b = row_b.get('valor_parcela_num', 0)
                    total_t = row_t.get('valor_total_num', 0)
                    total_b = row_b.get('valor_total_num', 0)
                    
                    if abs(val_t - val_b) > VALUE_TOL or abs(total_t - total_b) > VALUE_TOL:
                        status = "⚠️ VALORES DIVERGENTES"
                    else:
                        status = "⚠️ MATCH PARCIAL"

                # For the output, we only show the BGCARD instance once
                # We only output when we have a match and we're processing the TRIER row
                # that corresponds to the parcela number in BGCARD
                if match_type == "full" or match_type == "parcela_divergent":
                    # Only output if this TRIER parcela matches the BGCARD parcela number
                    # or if it's a parcela divergent case
                    out.append([
                        format_value_for_json(row_t.get('filial', '')),
                        row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                        trier_name,
                        format_value_for_json(row_t.get('valor_parcela_num', 0)),
                        format_value_for_json(row_t.get('valor_total_num', 0)),
                        bg_name,
                        format_value_for_json(row_b.get('valor_parcela_num', 0)),
                        format_value_for_json(row_b.get('valor_total_num', 0)),
                        status,
                        ""  # Placeholder for annotations
                    ])

            # ---------- ONLY TRIER ----------
            else:
                # Check if this CPF has ANY matches in BGCARD (for partial matching)
                has_bg_matches = trier_cpf in bg_by_cpf
                
                if not has_bg_matches:
                    # Only show as SOMENTE TRIER if there are no BGCARD records for this CPF at all
                    trier_cliente = str(row_t.get('cliente_name', '')).strip()
                    
                    # Format CPF for display
                    cpf_display = trier_cpf
                    if len(cpf_display) == 11:
                        cpf_display = f"{cpf_display[:3]}.{cpf_display[3:6]}.{cpf_display[6:9]}-{cpf_display[9:]}"
                    
                    trier_parcela = f"{row_t.get('parcela_n', '?')}/{row_t.get('parcela_total', '?')}"
                    trier_name = f"{trier_cliente} — PARCELA {trier_parcela} ({cpf_display})" if trier_cliente else f"PARCELA {trier_parcela} ({cpf_display})"

                    out.append([
                        format_value_for_json(row_t.get('filial', '')),
                        row_t['data_emissao'].strftime("%d/%m/%Y") if row_t.get('data_emissao') else '',
                        trier_name,
                        format_value_for_json(row_t.get('valor_parcela_num', 0)),
                        format_value_for_json(row_t.get('valor_total_num', 0)),
                        "-",
                        "-",
                        "-",
                        "⚠️ SOMENTE TRIER",
                        ""  # Placeholder for annotations
                    ])

    # ---------- ONLY BGCARD ----------
    if not b.empty:
        for j, row_b in b.iterrows():
            if j in used_bg:
                continue

            bg_cliente = str(row_b.get('cliente_name', '')).strip()
            bg_cpf = row_b.get('cpf', '')
            
            # Format CPF for display
            cpf_display = bg_cpf
            if len(cpf_display) == 11:
                cpf_display = f"{cpf_display[:3]}.{cpf_display[3:6]}.{cpf_display[6:9]}-{cpf_display[9:]}"
            
            bg_parcela = f"{row_b.get('parcela_n', '?')}/{row_b.get('parcela_total', '?')}"
            bg_name = f"{bg_cliente} — PARCELA {bg_parcela} ({cpf_display})" if bg_cliente else f"PARCELA {bg_parcela} ({cpf_display})"

            out.append([
                format_value_for_json(row_b.get('filial', '')),
                row_b['data_emissao'].strftime("%d/%m/%Y") if row_b.get('data_emissao') else '',
                "-",
                "-",
                "-",
                bg_name,
                format_value_for_json(row_b.get('valor_parcela_num', 0)),
                format_value_for_json(row_b.get('valor_total_num', 0)),
                "⚠️ SOMENTE BGCARD",
                ""  # Placeholder for annotations
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
            cpf_match = re.search(r'\((\d{3}\.\d{3}\.\d{3}-\d{2})\)', trier_text + " " + bg_text)
            parcela_match = re.search(r'PARCELA (\d+)/(\d+)', trier_text + " " + bg_text)
            
            if cpf_match and parcela_match:
                cpf_digits = re.sub(r"\D", "", cpf_match.group(1))
                parcela_key = parcela_match.group(0)
                composite_key = f"{cpf_digits}|{parcela_key}"
                
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
