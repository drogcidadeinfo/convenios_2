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

HEADER = ["Filial", "CPF", "TRIER", "Valor", "MINERVA", "Valor", "STATUS"]

# tolerância de diferença de valor (para arredondamentos)
VALUE_TOL = 0.05


# ----------------------------
# Helpers
# ----------------------------
def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def normalize_colname(x: str) -> str:
    # remove NBSP, normaliza espaços, remove acentos, lowercase
    s = str(x).replace("\xa0", " ").strip()
    s = strip_accents(s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df

def name_tokens(name: str) -> set:
    if name is None:
        return set()
    s = strip_accents(str(name)).upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split(" ") if len(t) >= 2]
    return set(toks)

def parse_brl_money(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s == "-":
        return None
    # remove "R$", espaços e etc
    s = s.replace("R$", "").replace(" ", "")
    # milhar '.' e decimal ','
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def format_brl(v):
    if v is None or pd.isna(v):
        return "-"
    # formata 1234.5 -> "R$ 1.234,50"
    s = f"{float(v):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def parse_date_br(x):
    # tenta dd/mm/yyyy
    if x is None or str(x).strip() == "":
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).date()
    s = str(x).strip()
    d = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.date()

def token_match_score(tokens_a: set, tokens_b: set) -> int:
    # score simples: quantas palavras em comum
    if not tokens_a or not tokens_b:
        return 0
    return len(tokens_a.intersection(tokens_b))

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
# Core: build output rows
# ----------------------------
def build_conferencia_cpf_valor(df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[list]:
    a = normalize_df_columns(df_a)
    b = normalize_df_columns(df_b)

    # required columns
    for col in ["cliente", "cpf", "valor"]:
        if col not in a.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_TRIER}. Achei: {list(a.columns)}")
        if col not in b.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em {SHEET_MINERVA}. Achei: {list(b.columns)}")

    # Filial optional in each
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

    # drop invalid rows
    a = a.dropna(subset=["cpf_norm", "Valor_num"])
    b = b.dropna(subset=["cpf_norm", "Valor_num"])

    out_rows = []
    used_b = set()

    # index B by CPF for speed
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

            status = "✅ OK" if diff <= VALUE_TOL else "⚠️ VALOR"

            # Filial: prefer A if present, else B
            filial_out = a.at[i, "filial"]
            if pd.isna(filial_out):
                filial_out = b.at[best_j, "filial"]
            filial_out = "-" if pd.isna(filial_out) else int(filial_out)

            out_rows.append([
                filial_out,
                format_cpf(cpf),
                str(a.at[i, "cliente"]),
                format_brl(a_val),
                str(b.at[best_j, "cliente"]),
                format_brl(b_val),
                status
            ])
        else:
            filial_out = a.at[i, "filial"]
            filial_out = "-" if pd.isna(filial_out) else int(filial_out)

            out_rows.append([
                filial_out,
                format_cpf(cpf),
                str(a.at[i, "cliente"]),
                format_brl(a_val),
                "-",
                "-",
                "⚠️ SÓ A"
            ])

    # leftover B rows
    for j in range(len(b)):
        if j in used_b:
            continue

        cpf = b.at[j, "cpf_norm"]
        b_val = float(b.at[j, "Valor_num"])

        filial_out = b.at[j, "filial"]
        filial_out = "-" if pd.isna(filial_out) else int(filial_out)

        out_rows.append([
            filial_out,
            format_cpf(cpf),
            "-",
            "-",
            str(b.at[j, "cliente"]),
            format_brl(b_val),
            "⚠️ SÓ B"
        ])

    # sort by CPF then status
    def sort_key(r):
        return (r[1], r[6], r[2], r[4])

    out_rows.sort(key=sort_key)
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

def write_values_chunked(ws, values, start_cell="A1", chunk_size=500):
    # values is list of lists
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        start_row = 1 + i  # 1-indexed
        cell = f"A{start_row}"
        ws.update(cell, chunk, value_input_option="RAW")

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

    rows_cpf = build_conferencia_cpf_valor(df_t, df_m)

    ws_out = upsert_worksheet(sh, SHEET_OUT, rows=max(2000, len(rows_cpf) + 5), cols=10)
    ws_out.clear()

    values = [HEADER] + rows_cpf
    write_values_chunked(ws_out, values)

if __name__ == "__main__":
    main()
