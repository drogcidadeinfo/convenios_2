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
SHEET_TRIER = "dados_trier_alegrete"
SHEET_MINERVA = "dados_minerva_alegrete"
SHEET_OUT = "TRIERxMINERVA_ALEGRETE"

# Updated header - removed Filial
HEADER = ["CPF", "TRIER", "Valor", "MINERVA", "Valor", "STATUS", "Anotações"]

# tolerância de diferença de valor (para arredondamentos)
VALUE_TOL = 0.05

# Color mapping for STATUS
COLOR_MAP = {
    "✅ OK": {"red": 0.8, "green": 0.9, "blue": 0.8},  # Light green
    "⚠️ VALOR DIVERGENTE": {"red": 1.0, "green": 0.8, "blue": 0.8},  # Light red
    "⚠️ SOMENTE TRIER": {"red": 1.0, "green": 0.95, "blue": 0.8},  # Light yellow
    "⚠️ SOMENTE MINERVA": {"red": 0.9, "green": 0.9, "blue": 1.0},  # Light blue
}


# ----------------------------
# Helpers
# ----------------------------
def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def normalize_colname(x: str) -> str:
    s = str(x).replace("\xa0", " ").strip()
    s = strip_accents(s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df

def parse_brl_money(x):
    if x is None:
        return None

    if isinstance(x, (int, float)) and not pd.isna(x):
        return float(x)

    s = str(x).strip()
    if s == "" or s == "-":
        return None

    s = s.replace("R$", "").replace(" ", "")

    if "," in s:
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except ValueError:
            return None

    if re.fullmatch(r"\d+(\.\d+)?", s):
        try:
            return float(s)
        except ValueError:
            return None

    digits = re.sub(r"\D", "", s)
    if digits == "":
        return None

    try:
        n = float(digits)
    except ValueError:
        return None

    if len(digits) <= 3:
        return n
    return n / 100.0

def format_brl(v, show_difference=False):
    if v is None or pd.isna(v):
        return "-"
    
    if show_difference and v != 0:
        sign = "+" if v > 0 else "-"
        s = f"{abs(float(v)):,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{sign} R$ {s}"
    elif show_difference and v == 0:
        return "R$ 0,00"
    else:
        s = f"{float(v):,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"

def normalize_cpf(x):
    if x is None:
        return None
    s = re.sub(r"\D", "", str(x))
    if s == "":
        return None
    s = s.zfill(11)
    return s if len(s) == 11 else None

def format_cpf(cpf_digits: str) -> str:
    if not cpf_digits or len(cpf_digits) != 11:
        return "-"
    return re.sub(r"(\d{3})(\d{3})(\d{3})(\d{2})", r"\1.\2.\3-\4", cpf_digits)


# ----------------------------
# Core: build output rows (one per CPF)
# ----------------------------
def build_conferencia_cpf_valor(df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[dict]:
    """
    TRIER (A) may have multiple rows per CPF (split by Filial)
    MINERVA (B) has a single total per CPF

    Output: one row per CPF, with TRIER value showing sum or difference
    """
    a = normalize_df_columns(df_a)
    b = normalize_df_columns(df_b)

    for col in ["cliente", "cpf", "valor"]:
        if col not in a.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_TRIER}. Achei: {list(a.columns)}")
        if col not in b.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_MINERVA}. Achei: {list(b.columns)}")

    a["cpf_norm"] = a["cpf"].apply(normalize_cpf)
    b["cpf_norm"] = b["cpf"].apply(normalize_cpf)

    a["Valor_num"] = a["valor"].apply(parse_brl_money)
    b["Valor_num"] = b["valor"].apply(parse_brl_money)

    a = a.dropna(subset=["cpf_norm", "Valor_num"])
    b = b.dropna(subset=["cpf_norm", "Valor_num"])

    # ---- MINERVA: build dict cpf -> total (and client name)
    b_grp = (
        b.groupby("cpf_norm", as_index=False)
         .agg({"Valor_num": "sum", "cliente": "first"})
    )
    b_map = {row["cpf_norm"]: (float(row["Valor_num"]), str(row["cliente"])) for _, row in b_grp.iterrows()}

    # ---- TRIER: group by CPF and sum values
    a_grp = (
        a.groupby("cpf_norm", as_index=False)
         .agg({
             "Valor_num": "sum",
             "cliente": lambda x: " / ".join(sorted(set(str(v) for v in x if pd.notna(v))))  # Join multiple client names
         })
    )
    
    out = []
    used_minerva_cpfs = set()

    # Process each CPF from TRIER
    for _, row in a_grp.iterrows():
        cpf = row["cpf_norm"]
        trier_sum = float(row["Valor_num"])
        trier_nome = row["cliente"]
        
        minerva_info = b_map.get(cpf)

        if minerva_info is not None:
            minerva_total, minerva_cliente = minerva_info
            used_minerva_cpfs.add(cpf)

            diff = abs(trier_sum - minerva_total)
            
            if diff <= VALUE_TOL:
                # OK case - show the sum
                status = "✅ OK"
                trier_val_display = trier_sum
                trier_nome_display = trier_nome
            else:
                # Divergent case - show the difference
                status = "⚠️ VALOR DIVERGENTE"
                trier_val_display = trier_sum - minerva_total  # Positive if TRIER has more, negative if less
                trier_nome_display = trier_nome

            out.append({
                "cpf_digits": cpf,
                "trier_nome": trier_nome_display,
                "trier_val": trier_val_display,
                "minerva_nome": minerva_cliente,
                "minerva_val": minerva_total,
                "status_calc": status,
                "is_divergent": status == "⚠️ VALOR DIVERGENTE"
            })

        else:
            # CPF exists only in TRIER
            out.append({
                "cpf_digits": cpf,
                "trier_nome": trier_nome,
                "trier_val": trier_sum,
                "minerva_nome": "-",
                "minerva_val": None,
                "status_calc": "⚠️ SOMENTE TRIER",
                "is_divergent": False
            })

    # ---- leftover MINERVA-only CPFs
    for cpf, (minerva_total, minerva_cliente) in b_map.items():
        if cpf in used_minerva_cpfs:
            continue
        out.append({
            "cpf_digits": cpf,
            "trier_nome": "-",
            "trier_val": None,
            "minerva_nome": minerva_cliente,
            "minerva_val": minerva_total,
            "status_calc": "⚠️ SOMENTE MINERVA",
            "is_divergent": False
        })

    def sort_key(d):
        return (format_cpf(d["cpf_digits"]), d["status_calc"])

    out.sort(key=sort_key)
    return out


# ----------------------------
# Google Sheets I/O with formatting
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

def read_existing_annotations(ws_out) -> dict:
    """
    Read only the Anotações column, keyed by CPF only
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (7 - len(row))

        cpf_digits = normalize_cpf(row[0])  # CPF is now first column
        if not cpf_digits:
            continue

        # Get annotation from column G (index 6)
        anot = (row[6] or "").strip()
        
        # Store by CPF only
        if cpf_digits not in annotations and anot:
            annotations[cpf_digits] = anot
        elif anot and cpf_digits in annotations and not annotations[cpf_digits]:
            annotations[cpf_digits] = anot

    return annotations

def apply_status_coloring(ws, num_rows: int):
    """
    Apply background color to STATUS column based on the status value
    """
    try:
        requests = []
        
        # Get all status values to determine coloring
        status_range = f"F2:F{num_rows}"  # STATUS is now column F
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
                            "startColumnIndex": 5,  # Column F (0-based)
                            "endColumnIndex": 6
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

def write_values_chunked(ws, values, start_cell="A1", chunk_size=500):
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        start_row = 1 + i
        cell = f"A{start_row}"
        ws.update(cell, chunk, value_input_option="RAW")

def clear_leftover_rows(ws, start_row: int, end_row: int, end_col_letter: str):
    """
    Clears A{start_row}:{end_col}{end_row}
    """
    if end_row >= start_row:
        ws.batch_clear([f"A{start_row}:{end_col_letter}{end_row}"])

def main():
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_ID não definido no ambiente.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GSERVICE_JSON"]),
        scopes=scopes
    )

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_t = sh.worksheet(SHEET_TRIER)
    ws_m = sh.worksheet(SHEET_MINERVA)

    df_t = pd.DataFrame(ws_t.get_all_records())
    df_m = pd.DataFrame(ws_m.get_all_records())

    items = build_conferencia_cpf_valor(df_t, df_m)

    ws_out = upsert_worksheet(sh, SHEET_OUT, rows=max(2000, len(items) + 5), cols=10)
    ensure_sheet_size(ws_out, min_rows=max(2000, len(items) + 5), min_cols=7)  # Now 7 columns

    # Read only existing annotations
    annotations = read_existing_annotations(ws_out)

    rows = []
    for d in items:
        cpf_digits = d["cpf_digits"]
        
        # Format TRIER value - show difference for divergent cases
        is_divergent = d.get("is_divergent", False)
        trier_val_fmt = format_brl(d["trier_val"], show_difference=is_divergent)
        
        minerva_val_fmt = format_brl(d["minerva_val"]) if d["minerva_val"] is not None else "-"

        # Get annotation by CPF only
        anot = annotations.get(cpf_digits, "")

        # Always use calculated status
        status_final = d["status_calc"]

        rows.append([
            format_cpf(cpf_digits),
            d["trier_nome"],
            trier_val_fmt,
            d["minerva_nome"],
            minerva_val_fmt,
            status_final,
            anot  # preserve notes by CPF
        ])

    values = [HEADER] + rows

    # Write new table
    write_values_chunked(ws_out, values, chunk_size=500)

    # Clear leftovers
    prev_len = len(ws_out.get_all_values())
    new_len = len(values)
    if prev_len > new_len:
        clear_leftover_rows(ws_out, start_row=new_len + 1, end_row=prev_len, end_col_letter="G")  # Now up to column G

    # Apply status coloring
    apply_status_coloring(ws_out, len(rows))

    # Remove any existing data validation from STATUS column
    try:
        # Clear data validation from column F (index 5)
        ws_out.clear_basic_filter()
        requests = [{
            "setDataValidation": {
                "range": {
                    "sheetId": ws_out.id,
                    "startRowIndex": 0,
                    "endRowIndex": ws_out.row_count,
                    "startColumnIndex": 5,  # Column F
                    "endColumnIndex": 6
                },
                "rule": None  # This removes the validation
            }
        }]
        sh.batch_update({"requests": requests})
    except Exception as e:
        print(f"Note: Could not remove data validation: {e}")

if __name__ == "__main__":
    main()
