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
SHEET_TRIER = "dados_trier"
SHEET_CREDCOM = "dados_cred_commerce"
SHEET_OUT = "conferencia_diaria"

HEADER = ["Filial", "Data Emissão", "TRIER", "Valor", "CREDCOM", "Valor", "STATUS"]

# tolerância de diferença de valor (para arredondamentos)
VALUE_TOL = 0.05

# ----------------------------
# Helpers
# ----------------------------
def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

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
    return pd.to_datetime(s, dayfirst=True, errors="coerce").date()

def token_match_score(tokens_a: set, tokens_b: set) -> int:
    # score simples: quantas palavras em comum
    if not tokens_a or not tokens_b:
        return 0
    return len(tokens_a.intersection(tokens_b))

# ----------------------------
# Core: build output rows
# ----------------------------
def build_conferencia_rows(df_trier: pd.DataFrame, df_cred: pd.DataFrame) -> list[list]:
    # Normaliza colunas esperadas
    # Filial | Cliente | Data Emissão | Valor
    required_cols = ["Filial", "Cliente", "Data Emissão", "Parcela", "Valor"]
    for c in required_cols:
        if c not in df_trier.columns:
            raise ValueError(f"Coluna '{c}' não encontrada em {SHEET_TRIER}")
        if c not in df_cred.columns:
            raise ValueError(f"Coluna '{c}' não encontrada em {SHEET_CREDCOM}")

    t = df_trier.copy()
    c = df_cred.copy()

    t["Filial"] = pd.to_numeric(t["Filial"], errors="coerce").astype("Int64")
    c["Filial"] = pd.to_numeric(c["Filial"], errors="coerce").astype("Int64")

    t["Data_Emissao"] = t["Data Emissão"].apply(parse_date_br)
    c["Data_Emissao"] = c["Data Emissão"].apply(parse_date_br)

    t["Valor_num"] = t["Valor"].apply(parse_brl_money)
    c["Valor_num"] = c["Valor"].apply(parse_brl_money)

    t["tokens"] = t["Cliente"].apply(name_tokens)
    c["tokens"] = c["Cliente"].apply(name_tokens)

    # remove linhas inválidas (sem filial ou data)
    t = t.dropna(subset=["Filial", "Data_Emissao"])
    c = c.dropna(subset=["Filial", "Data_Emissao"])

    # agrupa por (Filial, Data)
    out_rows = []

    keys = sorted(set(zip(t["Filial"].astype(int), t["Data_Emissao"])) |
                  set(zip(c["Filial"].astype(int), c["Data_Emissao"])),
                  key=lambda x: (x[1], x[0]))  # (date, filial)

    for filial, dt in keys:
        tg = t[(t["Filial"].astype(int) == filial) & (t["Data_Emissao"] == dt)].reset_index(drop=True)
        cg = c[(c["Filial"].astype(int) == filial) & (c["Data_Emissao"] == dt)].reset_index(drop=True)

        used_c = set()

        # Para cada TRIER, achar melhor CREDCOM não usado
        for i in range(len(tg)):
            best_j = None
            best_score = 0

            for j in range(len(cg)):
                if j in used_c:
                    continue
                score = token_match_score(tg.at[i, "tokens"], cg.at[j, "tokens"])
                if score > best_score:
                    best_score = score
                    best_j = j

            # precisa de pelo menos 2 palavras em comum
            if best_j is not None and best_score >= 2:
                used_c.add(best_j)

                t_name = str(tg.at[i, "Cliente"])
                t_val = tg.at[i, "Valor_num"]

                c_name = str(cg.at[best_j, "Cliente"])
                c_val = cg.at[best_j, "Valor_num"]

                if (t_val is not None) and (c_val is not None) and abs(float(t_val) - float(c_val)) <= VALUE_TOL:
                    status = "✅ OK"
                else:
                    status = "⚠️ VALOR"

                out_rows.append([
                    filial,
                    dt.strftime("%d/%m/%Y"),
                    t_name,
                    format_brl(t_val),
                    c_name,
                    format_brl(c_val),
                    status
                ])
            else:
                # sem match no CREDCOM
                t_name = str(tg.at[i, "Cliente"])
                t_val = tg.at[i, "Valor_num"]
                out_rows.append([
                    filial,
                    dt.strftime("%d/%m/%Y"),
                    t_name,
                    format_brl(t_val),
                    "-",
                    "-",
                    "⚠️ SÓ TRIER"
                ])

        # sobrou CREDCOM sem par
        for j in range(len(cg)):
            if j in used_c:
                continue
            c_name = str(cg.at[j, "Cliente"])
            c_val = cg.at[j, "Valor_num"]
            out_rows.append([
                filial,
                dt.strftime("%d/%m/%Y"),
                "-",
                "-",
                c_name,
                format_brl(c_val),
                "⚠️ SÓ CREDCOM"
            ])

    # ordena para ficar bem “conferência”
    def sort_key(r):
        # r: [Filial, Data Emissão, ...]
        d = pd.to_datetime(r[1], dayfirst=True, errors="coerce")
        f = int(r[0]) if r[0] != "-" else 9999
        return (d, f, str(r[2]), str(r[4]))

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
    # chunk updates to avoid limits
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i+chunk_size]
        start_row = 1 + i  # 1-indexed
        cell = f"A{start_row}"
        ws.update(cell, chunk, value_input_option="RAW")

def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GSERVICE_JSON"]),
        scopes=scopes
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_t = sh.worksheet(SHEET_TRIER)
    ws_c = sh.worksheet(SHEET_CREDCOM)

    df_trier = pd.DataFrame(ws_t.get_all_records())
    df_cred = pd.DataFrame(ws_c.get_all_records())

    rows = build_conferencia_rows(df_trier, df_cred)

    ws_out = upsert_worksheet(sh, SHEET_OUT, rows=max(2000, len(rows) + 5), cols=10)
    ws_out.clear()

    values = [HEADER] + rows
    write_values_chunked(ws_out, values)

if __name__ == "__main__":
    main()
