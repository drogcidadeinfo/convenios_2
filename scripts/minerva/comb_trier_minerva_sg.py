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

def build_row_key(filial, cpf_digits: str) -> str:
    cpf_digits = normalize_cpf(cpf_digits) or ""
    filial_s = "" if filial is None or pd.isna(filial) else str(int(filial))
    return f"{cpf_digits}|{filial_s}"

# ----------------------------
# Core: build output rows
# ----------------------------
def build_conferencia_cpf_valor(df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[dict]:
    """
    TRIER (A) may have multiple rows per CPF (split by Filial)
    MINERVA (B) has a single total per CPF

    Output: one row per TRIER line (plus leftover MINERVA-only CPFs)
    """
    a = normalize_df_columns(df_a)
    b = normalize_df_columns(df_b)

    for col in ["cliente", "cpf", "valor"]:
        if col not in a.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_TRIER}. Achei: {list(a.columns)}")
        if col not in b.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_MINERVA}. Achei: {list(b.columns)}")

    # Filial exists only in TRIER
    if "filial" not in a.columns:
        a["filial"] = pd.NA

    a["filial"] = pd.to_numeric(a["filial"], errors="coerce").astype("Int64")

    a["cpf_norm"] = a["cpf"].apply(normalize_cpf)
    b["cpf_norm"] = b["cpf"].apply(normalize_cpf)

    a["Valor_num"] = a["valor"].apply(parse_brl_money)
    b["Valor_num"] = b["valor"].apply(parse_brl_money)

    a = a.dropna(subset=["cpf_norm", "Valor_num"])
    b = b.dropna(subset=["cpf_norm", "Valor_num"])

    # ---- MINERVA: build dict cpf -> total (and client name)
    # If MINERVA has multiple rows same CPF, sum them (safe).
    b_grp = (
        b.groupby("cpf_norm", as_index=False)
         .agg({"Valor_num": "sum", "cliente": "first"})
    )
    b_map = {row["cpf_norm"]: (float(row["Valor_num"]), str(row["cliente"])) for _, row in b_grp.iterrows()}

    # ---- TRIER: group indexes by CPF and sum values
    out = []
    used_minerva_cpfs = set()

    a_by_cpf = {}
    for i in range(len(a)):
        cpf = a.at[i, "cpf_norm"]
        a_by_cpf.setdefault(cpf, []).append(i)

    for cpf, idxs in a_by_cpf.items():
        trier_sum = float(a.loc[idxs, "Valor_num"].sum())
        minerva_info = b_map.get(cpf)

        if minerva_info is not None:
            minerva_total, minerva_cliente = minerva_info
            used_minerva_cpfs.add(cpf)

            diff = abs(trier_sum - minerva_total)
            status_all = "✅ OK" if diff <= VALUE_TOL else "⚠️ VALOR DIVERGENTE"

            # Sort TRIER lines by filial then value/name for consistent display
            idxs_sorted = sorted(
                idxs,
                key=lambda i: (
                    -1 if pd.isna(a.at[i, "filial"]) else int(a.at[i, "filial"]),
                    float(a.at[i, "Valor_num"]),
                    str(a.at[i, "cliente"]),
                )
            )

            for k, i in enumerate(idxs_sorted):
                filial_out = a.at[i, "filial"]
                filial_out = None if pd.isna(filial_out) else int(filial_out)

                # show MINERVA columns only on first line of that CPF
                show_minerva = (k == 0)

                out.append({
                    "filial": filial_out,
                    "cpf_digits": cpf,
                    "trier_nome": str(a.at[i, "cliente"]),
                    "trier_val": float(a.at[i, "Valor_num"]),
                    "minerva_nome": minerva_cliente if show_minerva else "-",
                    "minerva_val": minerva_total if show_minerva else None,
                    "minerva_total_for_key": minerva_total,   # keep stable key even when display is "-"
                    "minerva_nome_for_key": minerva_cliente, # keep stable key even when display is "-"
                    "status_calc": status_all,
                })

        else:
            # CPF exists only in TRIER -> each line is SOMENTE TRIER
            idxs_sorted = sorted(
                idxs,
                key=lambda i: (
                    -1 if pd.isna(a.at[i, "filial"]) else int(a.at[i, "filial"]),
                    float(a.at[i, "Valor_num"]),
                    str(a.at[i, "cliente"]),
                )
            )

            for i in idxs_sorted:
                filial_out = a.at[i, "filial"]
                filial_out = None if pd.isna(filial_out) else int(filial_out)

                out.append({
                    "filial": filial_out,
                    "cpf_digits": cpf,
                    "trier_nome": str(a.at[i, "cliente"]),
                    "trier_val": float(a.at[i, "Valor_num"]),
                    "minerva_nome": "-",
                    "minerva_val": None,
                    "minerva_total_for_key": None,
                    "minerva_nome_for_key": "",
                    "status_calc": "⚠️ SOMENTE TRIER",
                })

    # ---- leftover MINERVA-only CPFs
    for cpf, (minerva_total, minerva_cliente) in b_map.items():
        if cpf in used_minerva_cpfs:
            continue
        out.append({
            "filial": None,
            "cpf_digits": cpf,
            "trier_nome": "-",
            "trier_val": None,
            "minerva_nome": minerva_cliente,
            "minerva_val": minerva_total,
            "minerva_total_for_key": minerva_total,
            "minerva_nome_for_key": minerva_cliente,
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

def read_existing_annotations(ws_out) -> dict:
    """
    Read only the Anotações column, keyed by CPF only (not by filial)
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (8 - len(row))

        cpf_digits = normalize_cpf(row[1])
        if not cpf_digits:
            continue

        # Get annotation from column H (index 7)
        anot = (row[7] or "").strip()
        
        # Store by CPF only (if there are multiple annotations for same CPF, keep the first non-empty one)
        if cpf_digits not in annotations and anot:
            annotations[cpf_digits] = anot
        elif anot and cpf_digits in annotations and not annotations[cpf_digits]:
            annotations[cpf_digits] = anot

    return annotations

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
    ensure_sheet_size(ws_out, min_rows=max(2000, len(items) + 5), min_cols=8)

    # Read only existing annotations (not status overrides)
    annotations = read_existing_annotations(ws_out)

    rows = []
    for d in items:
        filial_out = "-" if d["filial"] is None else d["filial"]

        cpf_digits = d["cpf_digits"]
        trier_nome = d["trier_nome"]
        minerva_nome = d["minerva_nome"]

        trier_val_fmt = format_brl(d["trier_val"])
        minerva_val_fmt = format_brl(d["minerva_val"]) if d["minerva_val"] is not None else "-"

        # Get annotation by CPF only
        anot = annotations.get(cpf_digits, "")

        # Always use calculated status (no user overrides)
        status_final = d["status_calc"]

        rows.append([
            filial_out,
            format_cpf(cpf_digits),
            trier_nome,
            trier_val_fmt,
            minerva_nome,
            minerva_val_fmt,
            status_final,
            anot  # preserve notes by CPF
        ])

    values = [HEADER] + rows

    # Write new table (without wiping the entire sheet)
    write_values_chunked(ws_out, values, chunk_size=500)

    # Clear leftovers (if previous run had more rows)
    prev_len = len(ws_out.get_all_values())
    new_len = len(values)
    if prev_len > new_len:
        clear_leftover_rows(ws_out, start_row=new_len + 1, end_row=prev_len, end_col_letter="H")

    # Remove any existing data validation (dropdown) from STATUS column
    try:
        # Clear data validation from column G (index 6)
        ws_out.clear_basic_filter()
        requests = [{
            "setDataValidation": {
                "range": {
                    "sheetId": ws_out.id,
                    "startRowIndex": 0,
                    "endRowIndex": ws_out.row_count,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "rule": None  # This removes the validation
            }
        }]
        sh.batch_update({"requests": requests})
    except Exception as e:
        print(f"Note: Could not remove data validation: {e}")

if __name__ == "__main__":
    main()
