import os, json, time, logging
import pandas as pd
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -----------------------
# Google Sheets helpers
# -----------------------
def get_gspread_client_from_secret():
    svc_json = os.environ["GSERVICE_JSON"]  # full json as string
    info = json.loads(svc_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def append_df_to_sheet(df: pd.DataFrame, spreadsheet_id: str, worksheet_name: str):
    gc = get_gspread_client_from_secret()
    sh = gc.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="30")

    # Create headers if sheet is empty
    existing_values = ws.get_all_values()
    if not existing_values:
        ws.append_row(df.columns.tolist(), value_input_option="RAW")

    # Append rows
    ws.append_rows(df.values.tolist(), value_input_option="RAW")
    logging.info(f"✅ Appended {len(df)} rows to {worksheet_name}")
    logging.info("\n" + df.head(5).to_string())

# -----------------------
# Selenium helpers
# -----------------------
def build_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--ignore-certificate-errors")
    return webdriver.Chrome(options=chrome_options)


def extract_for_account(account, target_day, test_day):
    """
    Returns a DataFrame with extracted rows for ONE account.
    """
    driver = build_driver()
    wait = WebDriverWait(driver, 15)

    try:
        logging.info(f"➡️  Starting account: {account['name']}")
        driver.get("http://167.99.154.117/login")

        # login type select
        select_element = wait.until(EC.presence_of_element_located((By.ID, "loginTipo")))
        Select(select_element).select_by_value("parceiro")

        wait.until(EC.element_to_be_clickable((By.ID, "loginUsuario"))).send_keys(account["username"])
        wait.until(EC.element_to_be_clickable((By.ID, "loginSenha"))).send_keys(account["password"])
        wait.until(EC.element_to_be_clickable((By.ID, "btnEntrar"))).click()

        time.sleep(5)

        # navigation (keep your locators)
        wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/nav[1]/div/ul/li[3]/a/span"))).click()
        wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/nav[1]/div/ul/li[3]/ul/li[3]/a/span"))).click()
        time.sleep(3)

        wait.until(EC.element_to_be_clickable((By.ID, "btnFiltro"))).click()
        time.sleep(1)

        # start date calendar
        wait.until(EC.element_to_be_clickable((By.ID, "FILTRO-DATA-INICIAL"))).click()
        time.sleep(1)

        table = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[1]/div[2]/table")))
        day_cells = table.find_elements(By.CSS_SELECTOR, "td[data-title]")

        for cell in day_cells:
            if cell.text.strip() == str(test_day):
                cell.click()
                break

        # end date calendar
        wait.until(EC.element_to_be_clickable((By.ID, "FILTRO-DATA-FINAL"))).click()
        time.sleep(1)

        table2 = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[9]/div[1]/div[2]/table")))
        day_cells2 = table2.find_elements(By.CSS_SELECTOR, "td[data-title]")

        for cell in day_cells2:
            if cell.text.strip() == str(target_day):
                cell.click()
                break

        # close calendar + save filter
        driver.find_element(By.TAG_NAME, "body").click()
        wait.until(EC.element_to_be_clickable((By.ID, "button-save-filter"))).click()
        time.sleep(4)

        # extract table rows
        table = wait.until(EC.presence_of_element_located((By.ID, "extrato-table")))

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#extrato-table tbody tr")))
        except TimeoutException:
            logging.warning(f"⚠️ No rows for {account['name']}")
            return pd.DataFrame()

        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

        extracted = []
        for row in rows:
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) >= 5:
                extracted.append({
                    "account": account["name"],
                    "local": tds[0].text.strip(),
                    "data": tds[1].text.strip(),
                    "vencimento": tds[2].text.strip(),
                    "parcela": tds[3].text.strip(),
                    "valor": tds[4].text.strip(),
                    "extraction_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "filter_start_day": str(test_day),
                    "filter_end_day": str(target_day),
                })

        return pd.DataFrame(extracted)

    finally:
        driver.quit()
        logging.info(f"🧹 Closed browser for account: {account['name']}")


def main():
    accounts = json.loads(os.environ["CDL_ACC_JSON"])
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    worksheet_name = os.environ.get("WORKSHEET_NAME", "dados_cred_commerce")

    yesterday = datetime.now() - timedelta(days=1)
    target_day = yesterday.day
    test_day = 1  # your start day

    all_dfs = []
    for acc in accounts:
        df_acc = extract_for_account(acc, target_day=target_day, test_day=test_day)
        if not df_acc.empty:
            all_dfs.append(df_acc)

    if not all_dfs:
        logging.warning("❌ No data extracted from any account.")
        return

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Map old column names to new spreadsheet header names
    column_mapping = {
        "account": "Filial",
        "local": "Cliente",
        "data": "Data Emissão",
        "parcela": "Parcela",
        "valor": "Valor"
    }
    
    # Rename columns
    final_df = final_df.rename(columns=column_mapping)
    
    # Keep only the columns you want, in correct order
    final_df = final_df[
        ["Filial", "Cliente", "Data Emissão", "Parcela", "Valor"]
    ]

    append_df_to_sheet(final_df, spreadsheet_id, worksheet_name)
    logging.info("✅ Done")


if __name__ == "__main__":
    main()
