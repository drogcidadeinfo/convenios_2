import re
import os
import json
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
SHEET_OUT = "TRIERxSIND"

HEADER = [
    "Filial",
    "Data Emissão",
    "TRIER",
    "Valor Parcela",
    "Valor Total",
    "BGCARD",
    "Valor Parcela",
    "Valor Total",
    "STATUS"
]

VALUE_TOL = 0.75   # tolerância
TRIER_DISCOUNT = 0.95  # 5% desconto


# ----------------------------
# Helpers
# ----------------------------
def normalize_colname(x):
    s = str(x).replace("\xa0", " ").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s)


def normalize_df_columns(df):
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df


def parse_brl_money(x):
    if x is None or str(x).strip() == "":
        return None
    s = str(x).replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None


def format_brl(v):
    if v is None:
        return "-"
    s = f"{float(v):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s


def parse_date_br(x):
    if not x:
        return None
    d = pd.to_datetime(x, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.date()


def parse_parcela(x):
    """
    1/5 -> (1, 5)
    """
    if not x:
        return (None, None)

    m = re.search(r"(\d+)\s*/\s*(\d+)", str(x))
    if not m:
        return (None, None)

    return int(m.group(1)), int(m.group(2))


def clean_cpf(x):
    return re.sub(r"\D", "", str(x))


# ----------------------------
# Core logic
# ----------------------------
def build_rows(df_trier, df_bg):

    t = normalize_df_columns(df_trier)
    b = normalize_df_columns(df_bg)

    # Standardize columns
    for df in (t, b):
        df["cpf"] = df["cpf"].apply(clean_cpf)
        df["data_emissao"] = df["data"].apply(parse_date_br)
        df["valor_parcela_num"] = df["valor parcela"].apply(parse_brl_money)
        df["valor_total_num"] = df["valor total"].apply(parse_brl_money)
        df[["parcela_n", "parcela_total"]] = df["parcela"].apply(
            lambda x: pd.Series(parse_parcela(x))
        )

    # Apply 5% discount to TRIER
    t["valor_parcela_num"] = t["valor_parcela_num"] * TRIER_DISCOUNT
    t["valor_total_num"] = t["valor_total_num"] * TRIER_DISCOUNT

    used_bg = set()
    out = []

    for i, row_t in t.iterrows():

        match_index = None

        for j, row_b in b.iterrows():

            if j in used_bg:
                continue

            # CPF must match
            if row_t["cpf"] != row_b["cpf"]:
                continue

            # parcela number must match
            if row_t["parcela_n"] != row_b["parcela_n"]:
                continue

            # compare values with tolerance
            if (
                abs(row_t["valor_parcela_num"] - row_b["valor_parcela_num"]) <= VALUE_TOL
                and
                abs(row_t["valor_total_num"] - row_b["valor_total_num"]) <= VALUE_TOL
            ):
                match_index = j
                break

        # ---------- MATCH FOUND ----------
        if match_index is not None:

            used_bg.add(match_index)
            row_b = b.loc[match_index]

            # check total parcelas
            if row_t["parcela_total"] != row_b["parcela_total"]:
                status = "⚠️ NUM DE PARCELAS DIVERGENTES"
            else:
                status = "✅ OK"

            trier_name = f'{row_t["cliente"]} — PARCELA {row_t["parcela_n"]}/{row_t["parcela_total"]}'
            bg_name = f'{row_b["cliente"]} — PARCELA {row_b["parcela_n"]}/{row_b["parcela_total"]}'

            out.append([
                row_t["filial"],
                row_t["data_emissao"].strftime("%d/%m/%Y"),
                trier_name,
                format_brl(row_t["valor_parcela_num"]),
                format_brl(row_t["valor_total_num"]),
                bg_name,
                format_brl(row_b["valor_parcela_num"]),
                format_brl(row_b["valor_total_num"]),
                status,
                ""
            ])

        # ---------- ONLY TRIER ----------
        else:

            trier_name = f'{row_t["cliente"]} — PARCELA {row_t["parcela_n"]}/{row_t["parcela_total"]}'

            out.append([
                row_t["filial"],
                row_t["data_emissao"].strftime("%d/%m/%Y"),
                trier_name,
                format_brl(row_t["valor_parcela_num"]),
                format_brl(row_t["valor_total_num"]),
                "-",
                "-",
                "-",
                "⚠️ SOMENTE TRIER",
                ""
            ])

    # ---------- ONLY BGCARD ----------
    for j, row_b in b.iterrows():
        if j in used_bg:
            continue

        bg_name = f'{row_b["cliente"]} — PARCELA {row_b["parcela_n"]}/{row_b["parcela_total"]}'

        out.append([
            row_b["filial"],
            row_b["data_emissao"].strftime("%d/%m/%Y"),
            "-",
            "-",
            "-",
            bg_name,
            format_brl(row_b["valor_parcela_num"]),
            format_brl(row_b["valor_total_num"]),
            "⚠️ SOMENTE BGCARD",
            ""
        ])

    return out


# ----------------------------
# Main
# ----------------------------
def main():

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GSERVICE_JSON"]),
        scopes=scopes
    )

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    df_trier = pd.DataFrame(sh.worksheet(SHEET_TRIER).get_all_records())
    df_bg = pd.DataFrame(sh.worksheet(SHEET_BGCARD).get_all_records())

    rows = build_rows(df_trier, df_bg)

    ws_out = sh.worksheet(SHEET_OUT) if SHEET_OUT in [w.title for w in sh.worksheets()] \
        else sh.add_worksheet(title=SHEET_OUT, rows=2000, cols=10)

    values = [HEADER] + rows
    ws_out.clear()
    ws_out.update("A1", values)


if __name__ == "__main__":
    main()
