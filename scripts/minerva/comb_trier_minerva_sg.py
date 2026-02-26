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
SHEET_TRIER = "dados_trier_sg"
SHEET_MINERVA = "dados_minerva_sg"
SHEET_OUT = "TRIERxMINERVA_SG"

# NEW: add "Anotações"
HEADER = ["Filial", "CPF", "TRIER", "Valor", "MINERVA", "Valor", "STATUS", "Anotações"]

# Dropdown options (edit if you want)
STATUS_OPTIONS = ["✅ OK", "⚠️ VALOR DIVERGENTE", "⚠️ SOMENTE TRIER", "⚠️ SOMENTE MINERVA"]

# tolerância de diferença de valor (para arredondamentos)
VALUE_TOL = 0.05


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

def format_brl(v):
    if v is None or pd.isna(v):
        return "-"
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

def _norm_name_for_key(s: str) -> str:
    if s is None:
        return ""
    s = strip_accents(str(s)).upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_row_key(cpf_digits: str, trier_nome: str, trier_val, minerva_nome: str, minerva_val) -> str:
    """
    Key stable enough to match the same record between runs,
    while still distinguishing same-CPF multiple rows.
    """
    cpf_digits = normalize_cpf(cpf_digits) or ""
    t_nome = _norm_name_for_key(trier_nome)
    m_nome = _norm_name_for_key(minerva_nome)
    t_val = parse_brl_money(trier_val)
    m_val = parse_brl_money(minerva_val)
    t_val = "" if t_val is None else f"{t_val:.2f}"
    m_val = "" if m_val is None else f"{m_val:.2f}"
    return f"{cpf_digits}|{t_nome}|{t_val}|{m_nome}|{m_val}"

# ----------------------------
# Core: build output rows
# ----------------------------
def build_conferencia_cpf_valor(df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[dict]:
    """
    Returns list of dicts:
      {filial_out, cpf_digits, trier_nome, trier_val_num, minerva_nome, minerva_val_num, status_calc}
    """
    a = normalize_df_columns(df_a)
    b = normalize_df_columns(df_b)

    for col in ["cliente", "cpf", "valor"]:
        if col not in a.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_TRIER}. Achei: {list(a.columns)}")
        if col not in b.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_MINERVA}. Achei: {list(b.columns)}")

    if "filial" not in a.columns:
        a["filial"] = pd.NA
    if "filial" not in b.columns:
        b["filial"] = pd.NA

    a["filial"] = pd.to_numeric(a["filial"], errors="coerce").astype("Int64")
    b["filial"] = pd.to_numeric(b["filial"], errors="coerce").astype("Int64")

    a["cpf_norm"] = a["cpf"].apply(normalize_cpf)
    b["cpf_norm"] = b["cpf"].apply(normalize_cpf)

    a["Valor_num"] = a["valor"].apply(parse_brl_money)
    b["Valor_num"] = b["valor"].apply(parse_brl_money)

    a = a.dropna(subset=["cpf_norm", "Valor_num"])
    b = b.dropna(subset=["cpf_norm", "Valor_num"])

    out = []
    used_b = set()

    b_by_cpf = {}
    for j in range(len(b)):
        cpf = b.at[j, "cpf_norm"]
        b_by_cpf.setdefault(cpf, []).append(j)

    for i in range(len(a)):
        cpf = a.at[i, "cpf_norm"]
        a_val = float(a.at[i, "Valor_num"])

        candidates = [j for j in b_by_cpf.get(cpf, []) if j not in used_b]

        best_j = None
        best_diff = None
        for j in candidates:
            b_val = float(b.at[j, "Valor_num"])
            diff = abs(a_val - b_val)
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_j = j

        if best_j is not None:
            used_b.add(best_j)

            b_val = float(b.at[best_j, "Valor_num"])
            diff = abs(a_val - b_val)

            status = "✅ OK" if diff <= VALUE_TOL else "⚠️ VALOR DIVERGENTE"

            filial_out = a.at[i, "filial"]
            if pd.isna(filial_out):
                filial_out = b.at[best_j, "filial"]
            filial_out = None if pd.isna(filial_out) else int(filial_out)

            out.append({
                "filial": filial_out,
                "cpf_digits": cpf,
                "trier_nome": str(a.at[i, "cliente"]),
                "trier_val": a_val,
                "minerva_nome": str(b.at[best_j, "cliente"]),
                "minerva_val": b_val,
                "status_calc": status,
            })
        else:
            filial_out = a.at[i, "filial"]
            filial_out = None if pd.isna(filial_out) else int(filial_out)

            out.append({
                "filial": filial_out,
                "cpf_digits": cpf,
                "trier_nome": str(a.at[i, "cliente"]),
                "trier_val": a_val,
                "minerva_nome": "-",
                "minerva_val": None,
                "status_calc": "⚠️ SOMENTE TRIER",
            })

    for j in range(len(b)):
        if j in used_b:
            continue

        cpf = b.at[j, "cpf_norm"]
        b_val = float(b.at[j, "Valor_num"])

        filial_out = b.at[j, "filial"]
        filial_out = None if pd.isna(filial_out) else int(filial_out)

        out.append({
            "filial": filial_out,
            "cpf_digits": cpf,
            "trier_nome": "-",
            "trier_val": None,
            "minerva_nome": str(b.at[j, "cliente"]),
            "minerva_val": b_val,
            "status_calc": "⚠️ SOMENTE MINERVA",
        })

    def sort_key(d):
        return (format_cpf(d["cpf_digits"]), d["status_calc"], d["trier_nome"], d["minerva_nome"])

    out.sort(key=sort_key)
    return out

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

def read_existing_overrides(ws_out) -> dict:
    """
    Reads existing rows and returns:
      key -> (status_user, anotacoes_user)
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    hdr = values[0]
    # Accept both old header (7 cols) and new (8 cols)
    # Expected positions:
    # 0 Filial, 1 CPF, 2 TRIER, 3 Valor, 4 MINERVA, 5 Valor, 6 STATUS, 7 Anotações
    overrides = {}
    for row in values[1:]:
        row = row + [""] * (8 - len(row))  # pad
        cpf_digits = normalize_cpf(row[1])
        key = build_row_key(
            cpf_digits,
            row[2],
            row[3],
            row[4],
            row[5],
        )
        status_user = (row[6] or "").strip()
        anot = (row[7] or "").strip()
        # Only store if there is something meaningful (but storing blank is fine too)
        overrides[key] = (status_user, anot)
    return overrides

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

def apply_status_dropdown(sh, ws, start_row: int, end_row: int):
    """
    Applies data validation to STATUS column (G) from start_row..end_row
    """
    # column G => index 6 (0-based)
    sheet_id = ws.id
    requests = [{
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row - 1,   # 0-based, inclusive
                "endRowIndex": end_row,           # 0-based, exclusive
                "startColumnIndex": 6,            # G
                "endColumnIndex": 7
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": s} for s in STATUS_OPTIONS]
                },
                "showCustomUi": True,
                "strict": True
            }
        }
    }]
    sh.batch_update({"requests": requests})

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
    ensure_sheet_size(ws_out, min_rows=max(2000, len(items) + 5), min_cols=8)

    # NEW: load existing STATUS/Anotações so we don't overwrite user edits
    overrides = read_existing_overrides(ws_out)

    rows = []
    for d in items:
        filial_out = "-" if d["filial"] is None else d["filial"]

        cpf_digits = d["cpf_digits"]
        trier_nome = d["trier_nome"]
        minerva_nome = d["minerva_nome"]

        trier_val_fmt = format_brl(d["trier_val"])
        minerva_val_fmt = format_brl(d["minerva_val"]) if d["minerva_val"] is not None else "-"

        key = build_row_key(cpf_digits, trier_nome, trier_val_fmt, minerva_nome, minerva_val_fmt)

        status_calc = d["status_calc"]
        status_user, anot_user = overrides.get(key, ("", ""))

        # If user already changed status, keep it; else use calculated
        status_final = status_user if status_user else status_calc

        rows.append([
            filial_out,
            format_cpf(cpf_digits),
            trier_nome,
            trier_val_fmt,
            minerva_nome,
            minerva_val_fmt,
            status_final,
            anot_user  # preserve notes
        ])

    values = [HEADER] + rows

    # Write new table (without wiping the entire sheet)
    write_values_chunked(ws_out, values, chunk_size=500)

    # Clear leftovers (if previous run had more rows)
    prev_len = len(ws_out.get_all_values())
    new_len = len(values)
    if prev_len > new_len:
        clear_leftover_rows(ws_out, start_row=new_len + 1, end_row=prev_len, end_col_letter="H")

    # Apply dropdown to STATUS column for the rows we wrote
    if len(values) >= 2:
        apply_status_dropdown(sh, ws_out, start_row=2, end_row=len(values))

if __name__ == "__main__":
    main()
