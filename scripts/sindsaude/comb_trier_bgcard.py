import re, os, json
import unicodedata
from datetime import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ----------------------------
# Config
# ----------------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_BGCARD = "dados_bgcard"  
SHEET_TRIER = "dados_trier_sind"    
SHEET_OUT = "BGCARDxTRIER"

# Updated header to show both perspectives clearly
HEADER = ["CPF", "Cliente BGCARD", "Cliente TRIER", 
          "Filial BGCARD", "Filial TRIER",
          "Data 1ª Parcela BGCARD", "Data 1ª Parcela TRIER",
          "Parcelas BGCARD", "Parcelas TRIER", 
          "Valor Total BGCARD", "Valor Total TRIER",
          "Diferença Valor", "STATUS", "Anotações"]

# tolerância de diferença de valor (para arredondamentos)
VALUE_TOL = 0.10  # Slightly increased tolerance


# ----------------------------
# Helpers
# ----------------------------
def strip_accents(s: str) -> str:
    """Remove accents from string for better matching"""
    if pd.isna(s) or s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def normalize_colname(x: str) -> str:
    """Normalize column names for matching"""
    s = str(x).replace("\xa0", " ").strip()
    s = strip_accents(s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all column names in dataframe"""
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df

def clean_cpf(cpf: str) -> str:
    """Extract only digits from CPF"""
    if cpf is None or pd.isna(cpf):
        return ""
    # Remove "CPF" prefix if present
    cpf = str(cpf).replace("CPF", "").strip()
    return re.sub(r'\D', '', cpf)

def parse_brl_money(x):
    """Parse Brazilian currency format to float"""
    if x is None or pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s == "-":
        return None
    # Remove "R$" if present
    s = s.replace("R$", "").replace(" ", "")
    # Handle Brazilian format: 1.234,56 -> 1234.56
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def format_brl(v):
    """Format float to Brazilian currency"""
    if v is None or pd.isna(v):
        return "-"
    s = f"{float(v):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def parse_date_br(x):
    """Parse Brazilian date format (dd/mm/yyyy)"""
    if x is None or pd.isna(x) or str(x).strip() == "":
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).date()
    s = str(x).strip()
    try:
        d = pd.to_datetime(s, format='%d/%m/%Y', errors='coerce')
        return None if pd.isna(d) else d.date()
    except:
        return None

def parse_parcela_info(x):
    """
    Extract parcel number and total from string like "1/5"
    Returns (parcela_num, parcela_total)
    """
    if x is None or pd.isna(x):
        return (None, None)
    s = str(x).strip()
    m = re.search(r'(\d+)\s*/\s*(\d+)', s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (None, None)

def clean_name(name):
    """Clean name by removing special characters and normalizing"""
    if pd.isna(name) or name is None:
        return ""
    name = str(name)
    # Remove special characters but keep letters and spaces
    name = re.sub(r'[^\w\s]', '', name)
    # Remove extra spaces
    name = re.sub(r'\s+', ' ', name).strip()
    return name


# ----------------------------
# Core: build output rows
# ----------------------------
def build_conferencia_rows(df_bgcard: pd.DataFrame, df_trier: pd.DataFrame) -> list[list]:
    # Normalize column names
    b = normalize_df_columns(df_bgcard)
    t = normalize_df_columns(df_trier)
    
    # Check required columns (note: column is 'cpf' not 'cpf')
    bgcard_cols = ["data", "cpf", "cliente", "filial", "parcela", "valor total"]
    trier_cols = ["data", "cpf", "cliente", "filial", "parcela", "valor total"]
    
    for col in bgcard_cols:
        if col not in b.columns:
            print(f"Warning: Column '{col}' not found in BGCARD. Found: {list(b.columns)}")
    
    for col in trier_cols:
        if col not in t.columns:
            print(f"Warning: Column '{col}' not found in TRIER. Found: {list(t.columns)}")
    
    # Parse data
    b["data_parsed"] = b["data"].apply(parse_date_br)
    t["data_parsed"] = t["data"].apply(parse_date_br)
    
    b["cpf_clean"] = b["cpf"].apply(clean_cpf)
    t["cpf_clean"] = t["cpf"].apply(clean_cpf)
    
    b["valor_parsed"] = b["valor total"].apply(parse_brl_money)
    t["valor_parsed"] = t["valor total"].apply(parse_brl_money)
    
    # Parse parcel info
    b[["parcela_num", "parcela_total"]] = b["parcela"].apply(lambda x: pd.Series(parse_parcela_info(x)))
    t[["parcela_num", "parcela_total"]] = t["parcela"].apply(lambda x: pd.Series(parse_parcela_info(x)))
    
    # Clean names for display
    b["cliente_clean"] = b["cliente"].apply(clean_name)
    t["cliente_clean"] = t["cliente"].apply(clean_name)
    
    # Remove rows without CPF
    b = b[b["cpf_clean"] != ""]
    t = t[t["cpf_clean"] != ""]
    
    # Process BGCARD (first image) - group by CPF since it only has first installment
    bgcard_grouped = b.groupby("cpf_clean").agg({
        "cliente_clean": "first",
        "filial": "first",
        "data_parsed": "first",  # Date of first installment
        "parcela_total": "first",  # Total number of parcels
        "valor_parsed": "sum",  # Sum all values (but since it's first installment only, this should be total)
        "parcela_num": "count"  # Count of rows (should be 1)
    }).rename(columns={
        "parcela_num": "qtd_linhas_bgcard",
        "valor_parsed": "valor_total_bgcard"
    }).reset_index()
    
    # Process TRIER (second image) - group by CPF since it has all installments
    trier_grouped = t.groupby("cpf_clean").agg({
        "cliente_clean": "first",
        "filial": "first",
        "data_parsed": "min",  # First installment date
        "parcela_total": "first",  # Should be consistent
        "valor_parsed": "sum",  # Sum of all installments = total value
        "parcela_num": lambda x: list(x),  # List of parcel numbers
        "parcela": lambda x: list(x)  # List of parcel strings
    }).rename(columns={
        "valor_parsed": "valor_total_trier",
        "parcela_num": "parcelas_list",
        "parcela": "parcelas_str_list"
    }).reset_index()
    
    # Calculate parcel count for TRIER
    trier_grouped["qtd_parcelas_trier"] = trier_grouped["parcelas_list"].apply(len)
    
    # Merge both datasets on CPF
    merged = pd.merge(bgcard_grouped, trier_grouped, on="cpf_clean", how="outer", suffixes=('_bgcard', '_trier'))
    
    out_rows = []
    
    for _, row in merged.iterrows():
        cpf = row["cpf_clean"]
        
        # BGCARD data
        bgcard_cliente = row.get("cliente_clean_bgcard", "")
        bgcard_filial = row.get("filial_bgcard")
        bgcard_data = row.get("data_parsed_bgcard")
        bgcard_parcelas = row.get("parcela_total_bgcard")
        bgcard_valor = row.get("valor_total_bgcard")
        
        # TRIER data
        trier_cliente = row.get("cliente_clean_trier", "")
        trier_filial = row.get("filial_trier")
        trier_data = row.get("data_parsed_trier")
        trier_parcelas = row.get("parcela_total_trier")
        trier_valor = row.get("valor_total_trier")
        trier_parcelas_count = row.get("qtd_parcelas_trier", 0)
        
        # Determine status
        status_parts = []
        
        # Check if exists in both
        if pd.notna(bgcard_valor) and pd.notna(trier_valor):
            # Compare parcel count
            if bgcard_parcelas == trier_parcelas_count:
                status_parts.append("✅ Parcelas OK")
            else:
                status_parts.append(f"⚠️ Parcelas: BGCARD={bgcard_parcelas} vs TRIER={trier_parcelas_count}")
            
            # Compare values
            diff = abs(bgcard_valor - trier_valor)
            diff_percent = (diff / bgcard_valor * 100) if bgcard_valor else 0
            
            if diff <= VALUE_TOL:
                status_parts.append("✅ Valor OK")
            else:
                status_parts.append(f"⚠️ Valor diff: R$ {diff:.2f} ({diff_percent:.1f}%)")
                
        elif pd.notna(bgcard_valor):
            status_parts.append("⚠️ Apenas no BGCARD")
        elif pd.notna(trier_valor):
            status_parts.append("⚠️ Apenas no TRIER")
        
        status = " | ".join(status_parts) if status_parts else "⚠️ Sem dados"
        
        # Format dates
        bgcard_data_str = bgcard_data.strftime("%d/%m/%Y") if bgcard_data else "-"
        trier_data_str = trier_data.strftime("%d/%m/%Y") if trier_data else "-"
        
        # Format parcel info for TRIER (show all parcels)
        trier_parcelas_display = ", ".join([str(p) for p in row.get("parcelas_str_list", [])]) if pd.notna(trier_parcelas_count) else "-"
        
        # Calculate difference
        if pd.notna(bgcard_valor) and pd.notna(trier_valor):
            diff_valor = trier_valor - bgcard_valor
            diff_display = format_brl(diff_valor)
        else:
            diff_display = "-"
        
        out_rows.append([
            cpf,
            bgcard_cliente if bgcard_cliente else "-",
            trier_cliente if trier_cliente else "-",
            bgcard_filial if pd.notna(bgcard_filial) else "-",
            trier_filial if pd.notna(trier_filial) else "-",
            bgcard_data_str,
            trier_data_str,
            f"{bgcard_parcelas if pd.notna(bgcard_parcelas) else '-'} parcela(s)",
            trier_parcelas_display,
            format_brl(bgcard_valor) if pd.notna(bgcard_valor) else "-",
            format_brl(trier_valor) if pd.notna(trier_valor) else "-",
            diff_display,
            status,
            ""  # Anotações (to be filled manually)
        ])
    
    return out_rows


# ----------------------------
# Google Sheets I/O
# ----------------------------
def upsert_worksheet(sh, title: str, rows: int = 2000, cols: int = 10):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    return ws

def ensure_sheet_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows:
        ws.resize(rows=min_rows)
    if ws.col_count < min_cols:
        ws.resize(cols=min_cols)

def write_values_chunked(ws, values, start_cell="A1", chunk_size=500):
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        start_row = 1 + i
        cell = f"A{start_row}"
        ws.update(cell, chunk, value_input_option="RAW")

def clear_leftover_rows(ws, start_row: int, end_row: int, end_col_letter: str):
    if end_row >= start_row:
        ws.batch_clear([f"A{start_row}:{end_col_letter}{end_row}"])

def main():
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_ID não definido no ambiente.")
    
    # For testing without Google Sheets
    if os.getenv("TEST_MODE") == "1":
        print("Running in test mode...")
        # Create sample data matching your images
        bgcard_data = {
            'Data': ['01/02/2026', '02/02/2026', '02/02/2026'],
            'cpf': ['031.089.420-42', '037.372.030-08', '041.301.020-10'],
            'Cliente': ['Fernanda Soltau Marques', 'Piúzma Roberta Barcelos de Oliveira', 'Thaiáás Martins Vieira'],
            'Filial': [1, 1, 1],
            'Parcela': ['1/3', '1/5', '1/3'],
            'Valor Parcela': [25.33, 21.78, 32.31],
            'Valor Total': [75.99, 108.9, 96.93]
        }
        
        trier_data = {
            'Data': ['01/02/2026', '01/02/2026', '02/02/2026', '02/02/2026', '02/02/2026'],
            'cpf': ['042.927.450-56', '042.927.450-56', '037.372.030-08', '037.372.030-08', '041.301.020-10'],
            'Cliente': ['EWERTON MUNHOZ GONCALVES', 'EWERTON MUNHOZ GONCALVES', 
                       'PAMELA ROBERTA GONÇALVES DE OLIVEIRA', 'PAMELA ROBERTA GONÇALVES DE OLIVEIRA',
                       'THAIS MARTINS VIEIRA'],
            'Filial': [2, 2, 2, 2, 2],
            'Parcela': ['1/3', '3/3', '4/5', '3/5', '1/3'],
            'Valor Parcela': [24.06, 24.06, 20.69, 20.69, 30.7],
            'Valor Total': [72.18, 72.18, 103.45, 103.45, 92.1]
        }
        
        df_bgcard = pd.DataFrame(bgcard_data)
        df_trier = pd.DataFrame(trier_data)
        
        items = build_conferencia_rows(df_bgcard, df_trier)
        
        # Print results
        print("\n" + "="*100)
        print("RESULTADO DA CONFERÊNCIA")
        print("="*100)
        
        for row in items:
            print(f"\nCPF: {row[0]}")
            print(f"BGCARD: {row[1]} (Filial {row[3]}, Data: {row[5]}, {row[7]})")
            print(f"TRIER:  {row[2]} (Filial {row[4]}, Data: {row[6]}, {row[8]})")
            print(f"Valores: BGCARD={row[9]}, TRIER={row[10]}, Diferença={row[11]}")
            print(f"STATUS: {row[12]}")
            print("-"*50)
        
        return
    
    # Regular Google Sheets execution
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GSERVICE_JSON"]),
        scopes=scopes
    )
    
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    ws_b = sh.worksheet(SHEET_BGCARD)
    ws_t = sh.worksheet(SHEET_TRIER)
    
    df_bgcard = pd.DataFrame(ws_b.get_all_records())
    df_trier = pd.DataFrame(ws_t.get_all_records())
    
    items = build_conferencia_rows(df_bgcard, df_trier)
    
    ws_out = upsert_worksheet(sh, SHEET_OUT, rows=max(2000, len(items) + 5), cols=20)
    ensure_sheet_size(ws_out, min_rows=max(2000, len(items) + 5), min_cols=14)
    
    values = [HEADER] + items
    write_values_chunked(ws_out, values, chunk_size=500)
    
    # Clear leftovers
    prev_len = len(ws_out.get_all_values())
    new_len = len(values)
    if prev_len > new_len:
        clear_leftover_rows(ws_out, start_row=new_len + 1, end_row=prev_len, end_col_letter="N")
    
    print(f"Conferência concluída! {len(items)} registros processados.")

if __name__ == "__main__":
    # Set TEST_MODE=1 to run without Google Sheets
    # os.environ["TEST_MODE"] = "1"
    main()
