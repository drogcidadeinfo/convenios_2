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
SHEET_TRIER = "dados_trier_sind"
SHEET_BGCARD = "dados_bgcard"
SHEET_OUT = "TRIERxBGCARD"

# Updated header with Anotações
HEADER = [
    "Filial",
    "CPF",
    "TRIER",
    "Valor TRIER",
    "BGCARD",
    "Valor BGCARD",
    "STATUS"
]

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

def parse_parcela_trier(x):
    """
    "PARCELA 8/10" -> (8, "PARCELA 8/10")
    "8/10" -> (8, "PARCELA 8/10")  (se vier sem a palavra)
    """
    if x is None:
        return (None, None)
    s = str(x).strip().upper()
    if not s or s == "-":
        return (None, None)

    m = re.search(r"(\d+)\s*/\s*(\d+)", s)
    if m:
        n = int(m.group(1))
        total = int(m.group(2))
        return (n, f"PARCELA {n}/{total}")

    # fallback: pega primeiro número
    m = re.search(r"\d+", s)
    if m:
        n = int(m.group(0))
        return (n, f"PARCELA {n}")
    return (None, None)

def parse_parcela_credcom(x):
    """
    7 -> (7, "PARCELA 7")
    "7" -> (7, "PARCELA 7")
    """
    if x is None:
        return (None, None)
    s = str(x).strip()
    if not s or s == "-":
        return (None, None)
    m = re.search(r"\d+", s)
    if not m:
        return (None, None)
    n = int(m.group(0))
    return (n, f"PARCELA {n}")

def build_row_key(filial, data_emissao, trier_nome, credcom_nome) -> str:
    """Build a unique key for a row to preserve annotations"""
    filial_str = "" if filial is None or pd.isna(filial) else str(int(filial))
    data_str = data_emissao.strftime("%Y%m%d") if isinstance(data_emissao, (datetime, pd.Timestamp)) else str(data_emissao)
    # Use combination of fields to create a stable key
    return f"{filial_str}|{data_str}|{trier_nome}|{credcom_nome}"

def clean_cpf(x):
    if x is None:
        return None
    s = re.sub(r"\D", "", str(x))
    return s if s else None


def parse_parcela_full(x):
    """
    '1/4' -> (1, 4)
    """
    if x is None:
        return (None, None)
    s = str(x).strip()
    m = re.search(r"(\d+)\s*/\s*(\d+)", s)
    if not m:
        return (None, None)
    return (int(m.group(1)), int(m.group(2)))

# ----------------------------
# Core: build output rows
# ----------------------------
def build_conferencia_rows(df_trier: pd.DataFrame, df_bg: pd.DataFrame):

    t = normalize_df_columns(df_trier)
    b = normalize_df_columns(df_bg)

    # Required columns
    required_trier = ["filial", "cliente", "cpf", "parcela", "valor parcela"]
    required_bg = ["filial", "cliente", "cpf", "parcela", "valor parcela"]

    for col in required_trier:
        if col not in t.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em dados_trier_sind")

    for col in required_bg:
        if col not in b.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em dados_bgcard")

    # Normalize
    t["cpf"] = t["cpf"].apply(clean_cpf)
    b["cpf"] = b["cpf"].apply(clean_cpf)

    t["Valor_num"] = t["valor parcela"].apply(parse_brl_money)
    b["Valor_num"] = b["valor parcela"].apply(parse_brl_money)

    # Adjust BGCARD 5%
    b["Valor_ajustado"] = b["Valor_num"] / 0.95

    t[["parcela_num", "parcela_total"]] = t["parcela"].apply(lambda x: pd.Series(parse_parcela_full(x)))
    b[["parcela_num", "parcela_total"]] = b["parcela"].apply(lambda x: pd.Series(parse_parcela_full(x)))

    out_rows = []

    all_cpfs = sorted(set(t["cpf"].dropna()) | set(b["cpf"].dropna()))

    for cpf in all_cpfs:

        tg = t[t["cpf"] == cpf].reset_index(drop=True)
        bg = b[b["cpf"] == cpf].reset_index(drop=True)

        used_bg = set()

        # Row-level match by parcela_num + value
        for i in range(len(tg)):
            best_j = None

            for j in range(len(bg)):
                if j in used_bg:
                    continue

                if tg.at[i, "parcela_num"] != bg.at[j, "parcela_num"]:
                    continue

                t_val = tg.at[i, "Valor_num"]
                b_val = bg.at[j, "Valor_ajustado"]

                if t_val is None or b_val is None:
                    continue

                if abs(t_val - b_val) <= VALUE_TOL:
                    best_j = j
                    break

            if best_j is not None:
                used_bg.add(best_j)
                status = "✅ OK"
                bg_nome = bg.at[best_j, "cliente"]
                bg_val_fmt = format_brl(bg.at[best_j, "Valor_ajustado"])
            else:
                status = "⚠️ NÃO ENCONTRADA NO BGCARD"
                bg_nome = "-"
                bg_val_fmt = "-"

            out_rows.append([
                tg.at[i, "filial"],
                cpf,
                tg.at[i, "cliente"] + f" — {tg.at[i,'parcela']}",
                format_brl(tg.at[i, "Valor_num"]),
                bg_nome,
                bg_val_fmt,
                status
            ])

        # Remaining BG not matched
        for j in range(len(bg)):
            if j in used_bg:
                continue

            out_rows.append([
                bg.at[j, "filial"],
                cpf,
                "-",
                "-",
                bg.at[j, "cliente"] + f" — {bg.at[j,'parcela']}",
                format_brl(bg.at[j, "Valor_ajustado"]),
                "⚠️ SOMENTE BGCARD"
            ])

        # Parcel quantity validation
        if len(tg) != len(bg):
            out_rows.append([
                "",
                cpf,
                "",
                "",
                "",
                "",
                "⚠️ PARCELAS INCOMPLETAS"
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

def read_existing_annotations(ws_out) -> dict:
    """
    Read the Anotações column and key by a combination of fields to preserve notes
    """
    values = ws_out.get_all_values()
    if not values:
        return {}

    annotations = {}
    for row in values[1:]:  # Skip header
        row = row + [""] * (8 - len(row))  # Pad to 8 columns

        filial_raw = (row[0] or "").strip()
        try:
            filial = int(filial_raw) if filial_raw not in ("", "-") else None
        except ValueError:
            filial = None
            
        data = row[1] if len(row) > 1 else ""
        trier_nome = row[2] if len(row) > 2 else ""
        credcom_nome = row[4] if len(row) > 4 else ""

        # Create a key from the identifying fields
        filial_str = "" if filial is None else str(filial)
        key = f"{filial_str}|{data}|{trier_nome}|{credcom_nome}"

        # Get annotation from column H (index 7)
        anot = (row[7] or "").strip()
        
        if anot:
            annotations[key] = anot

    return annotations

def to_python(value):
    """
    Convert numpy / pandas types to pure Python types
    """
    if pd.isna(value):
        return ""
    if hasattr(value, "item"):  # catches numpy types
        try:
            return value.item()
        except Exception:
            pass
    return value


def write_values_chunked(ws, values, start_cell="A1", chunk_size=500):
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]

        # 🔥 convert entire chunk to pure Python
        clean_chunk = [
            [to_python(cell) for cell in row]
            for row in chunk
        ]

        start_row = 1 + i
        cell = f"A{start_row}"

        ws.update(
            range_name=cell,
            values=clean_chunk,
            value_input_option="RAW"
        )

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
    ws_b = sh.worksheet(SHEET_BGCARD)

    df_trier = pd.DataFrame(ws_t.get_all_records())
    df_bg = pd.DataFrame(ws_b.get_all_records())

    rows = build_conferencia_rows(df_trier, df_bg)

    # Create / resize output sheet
    ws_out = upsert_worksheet(
        sh,
        SHEET_OUT,
        rows=max(2000, len(rows) + 5),
        cols=len(HEADER)
    )

    ensure_sheet_size(
        ws_out,
        min_rows=max(2000, len(rows) + 5),
        min_cols=len(HEADER)
    )

    # Sort rows by CPF then Filial
    def sort_key(r):
        filial = int(r[0]) if str(r[0]).isdigit() else 9999
        cpf = r[1] or ""
        return (cpf, filial)

    rows.sort(key=sort_key)

    values = [HEADER] + rows

    # Write table
    write_values_chunked(ws_out, values, chunk_size=500)

    # Clear leftovers
    prev_len = len(ws_out.get_all_values())
    new_len = len(values)

    if prev_len > new_len:
        clear_leftover_rows(
            ws_out,
            start_row=new_len + 1,
            end_row=prev_len,
            end_col_letter="G"
        )

    # Remove data validation from STATUS column
    try:
        ws_out.clear_basic_filter()
        requests = [{
            "setDataValidation": {
                "range": {
                    "sheetId": ws_out.id,
                    "startRowIndex": 0,
                    "endRowIndex": ws_out.row_count,
                    "startColumnIndex": 6,  # STATUS column
                    "endColumnIndex": 7
                },
                "rule": None
            }
        }]
        sh.batch_update({"requests": requests})
    except Exception as e:
        print(f"Note: Could not remove data validation: {e}")

if __name__ == "__main__":
    main()
