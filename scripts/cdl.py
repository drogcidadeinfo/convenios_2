import os
import json
import time
import logging
import traceback
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError

# Set up logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
)

# Create debug directory
DEBUG_DIR = "debug_screenshots"
os.makedirs(DEBUG_DIR, exist_ok=True)


# -----------------------
# Google Sheets helpers
# -----------------------
def get_gspread_client_from_secret():
    """Initialize Google Sheets client from service account JSON"""
    try:
        svc_json = os.environ.get("GSERVICE_JSON")
        if not svc_json:
            raise ValueError("GSERVICE_JSON environment variable not set")
        
        info = json.loads(svc_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse GSERVICE_JSON: {e}")
        raise
    except GoogleAuthError as e:
        logging.error(f"Google authentication failed: {e}")
        raise

def append_df_to_sheet(df: pd.DataFrame, spreadsheet_id: str, worksheet_name: str):
    """Append DataFrame to Google Sheets worksheet"""
    if df.empty:
        logging.warning("⚠️ DataFrame is empty, nothing to append")
        return
    
    try:
        gc = get_gspread_client_from_secret()
        sh = gc.open_by_key(spreadsheet_id)

        # Try to get existing worksheet or create new one
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="30")
            logging.info(f"Created new worksheet: {worksheet_name}")

        # Check if sheet is empty and add headers if needed
        existing_values = ws.get_all_values()
        if not existing_values:
            ws.append_row(df.columns.tolist(), value_input_option="RAW")
            logging.info(f"Added headers to {worksheet_name}")

        # Append rows in batches to avoid rate limits
        batch_size = 100
        rows_list = df.values.tolist()
        
        for i in range(0, len(rows_list), batch_size):
            batch = rows_list[i:i + batch_size]
            ws.append_rows(batch, value_input_option="RAW")
            logging.info(f"✅ Appended batch {i//batch_size + 1} ({len(batch)} rows)")
            time.sleep(1)  # Small delay to avoid rate limits
        
        logging.info(f"✅ Total: Appended {len(df)} rows to {worksheet_name}")
        logging.info("\n" + df.head(5).to_string())
        
    except gspread.exceptions.APIError as e:
        logging.error(f"Google Sheets API error: {e}")
        # Save locally as backup
        backup_file = f"backup_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(backup_file, index=False, encoding='utf-8-sig')
        logging.info(f"💾 Data saved locally to {backup_file}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in Google Sheets: {e}")
        raise


# -----------------------
# Selenium helpers
# -----------------------
def take_screenshot(driver, name: str):
    """Take screenshot for debugging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DEBUG_DIR}/{name}_{timestamp}.png"
    driver.save_screenshot(filename)
    logging.info(f"📸 Screenshot saved: {filename}")

def log_page_source(driver, name: str):
    """Save page source for debugging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DEBUG_DIR}/{name}_{timestamp}.html"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(driver.page_source)
    logging.info(f"📄 Page source saved: {filename}")

def safe_click(driver, by, selector, timeout=15, retries=3):
    """Safely click an element with retries"""
    for attempt in range(retries):
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((by, selector))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(0.5)
            element.click()
            return True
        except Exception as e:
            if attempt == retries - 1:
                logging.error(f"Failed to click {selector} after {retries} attempts: {e}")
                take_screenshot(driver, f"click_error_{selector.replace('/', '_')}")
                raise
            logging.warning(f"Click attempt {attempt + 1} failed for {selector}, retrying...")
            time.sleep(2 ** attempt)  # Exponential backoff
    return False

def build_driver():
    """Build Chrome driver with optimal settings"""
    chrome_options = webdriver.ChromeOptions()
    
    # Headless mode settings
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--log-level=3")  # Reduce logging
    
    # Additional stability options
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Performance options
    chrome_options.add_argument("--disable-javascript")  # Disable if not needed
    chrome_options.add_argument("--disable-images")  # Disable images for speed
    
    return webdriver.Chrome(options=chrome_options)

def wait_for_page_load(driver, timeout=15):
    """Wait for page to fully load"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception as e:
        logging.warning(f"Page load wait timeout: {e}")

def extract_for_account(account: Dict, target_day: int, test_day: int) -> pd.DataFrame:
    """
    Extract data for a single account with improved error handling
    """
    driver = None
    try:
        driver = build_driver()
        wait = WebDriverWait(driver, 15)
        
        logging.info(f"➡️  Processing account: {account['name']}")
        
        # Navigate to login page
        driver.get("http://167.99.154.117/login")
        wait_for_page_load(driver)
        take_screenshot(driver, f"{account['name']}_login_page")
        
        # Login process
        try:
            # Select login type
            select_element = wait.until(EC.presence_of_element_located((By.ID, "loginTipo")))
            Select(select_element).select_by_value("parceiro")
            
            # Enter credentials
            wait.until(EC.element_to_be_clickable((By.ID, "loginUsuario"))).send_keys(account["username"])
            wait.until(EC.element_to_be_clickable((By.ID, "loginSenha"))).send_keys(account["password"])
            
            # Click login button
            safe_click(driver, By.ID, "btnEntrar")
            time.sleep(5)
            take_screenshot(driver, f"{account['name']}_after_login")
            
        except Exception as e:
            logging.error(f"Login failed for {account['name']}: {e}")
            take_screenshot(driver, f"{account['name']}_login_error")
            log_page_source(driver, f"{account['name']}_login_error")
            return pd.DataFrame()
        
        # Navigation
        try:
            # Click main menu
            safe_click(driver, By.XPATH, "/html/body/nav[1]/div/ul/li[3]")
            time.sleep(2)
            
            # Click submenu
            safe_click(driver, By.XPATH, "/html/body/nav[1]/div/ul/li[3]/ul/li[3]")
            time.sleep(3)
            take_screenshot(driver, f"{account['name']}_after_navigation")
            
        except Exception as e:
            logging.error(f"Navigation failed for {account['name']}: {e}")
            take_screenshot(driver, f"{account['name']}_nav_error")
            return pd.DataFrame()
        
        # Click filter button
        safe_click(driver, By.ID, "btnFiltro")
        time.sleep(2)
        
        # Select start date
        try:
            safe_click(driver, By.ID, "FILTRO-DATA-INICIAL")
            time.sleep(2)
            
            # Wait for calendar and select date
            calendar = wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[8]/div[1]/div[2]/table")
            ))
            
            day_cells = calendar.find_elements(By.CSS_SELECTOR, "td[data-title]")
            start_date_selected = False
            
            for cell in day_cells:
                if cell.text.strip() == str(test_day):
                    driver.execute_script("arguments[0].scrollIntoView(true);", cell)
                    time.sleep(0.5)
                    cell.click()
                    start_date_selected = True
                    logging.info(f"✅ Selected start day: {test_day}")
                    break
            
            if not start_date_selected:
                logging.warning(f"⚠️ Start day {test_day} not found in calendar")
                take_screenshot(driver, f"{account['name']}_start_date_not_found")
                
        except Exception as e:
            logging.error(f"Start date selection failed: {e}")
            take_screenshot(driver, f"{account['name']}_start_date_error")
        
        # Select end date
        try:
            safe_click(driver, By.ID, "FILTRO-DATA-FINAL")
            time.sleep(2)
            
            calendar2 = wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[9]/div[1]/div[2]/table")
            ))
            
            day_cells2 = calendar2.find_elements(By.CSS_SELECTOR, "td[data-title]")
            end_date_selected = False
            
            for cell in day_cells2:
                if cell.text.strip() == str(target_day):
                    driver.execute_script("arguments[0].scrollIntoView(true);", cell)
                    time.sleep(0.5)
                    cell.click()
                    end_date_selected = True
                    logging.info(f"✅ Selected end day: {target_day}")
                    break
            
            if not end_date_selected:
                logging.warning(f"⚠️ End day {target_day} not found in calendar")
                take_screenshot(driver, f"{account['name']}_end_date_not_found")
                
        except Exception as e:
            logging.error(f"End date selection failed: {e}")
            take_screenshot(driver, f"{account['name']}_end_date_error")
        
        # Close calendar and save
        try:
            driver.find_element(By.TAG_NAME, "body").click()
            time.sleep(1)
            safe_click(driver, By.ID, "button-save-filter")
            time.sleep(5)
            take_screenshot(driver, f"{account['name']}_after_save")
            
        except Exception as e:
            logging.error(f"Save failed: {e}")
            take_screenshot(driver, f"{account['name']}_save_error")
        
        # Extract table data
        try:
            # Wait for table to load
            table = wait.until(EC.presence_of_element_located((By.ID, "extrato-table")))
            
            # Wait for rows with timeout
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#extrato-table tbody tr")))
            except TimeoutException:
                logging.warning(f"⚠️ No data rows found for {account['name']}")
                take_screenshot(driver, f"{account['name']}_empty_table")
                return pd.DataFrame()
            
            # Extract rows
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            extracted = []
            
            for row_idx, row in enumerate(rows, 1):
                try:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) >= 5:
                        data = {
                            "account": account["name"],
                            "local": tds[0].text.strip(),
                            "data": tds[1].text.strip(),
                            "vencimento": tds[2].text.strip(),
                            "parcela": tds[3].text.strip(),
                            "valor": tds[4].text.strip(),
                            "extraction_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "filter_start_day": str(test_day),
                            "filter_end_day": str(target_day),
                        }
                        extracted.append(data)
                except Exception as e:
                    logging.warning(f"Error extracting row {row_idx}: {e}")
                    continue
            
            logging.info(f"📊 Extracted {len(extracted)} rows for {account['name']}")
            return pd.DataFrame(extracted)
            
        except Exception as e:
            logging.error(f"Data extraction failed for {account['name']}: {e}")
            take_screenshot(driver, f"{account['name']}_extraction_error")
            return pd.DataFrame()
            
    except WebDriverException as e:
        logging.error(f"WebDriver error for {account['name']}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Unexpected error for {account['name']}: {e}")
        logging.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        if driver:
            driver.quit()
            logging.info(f"🧹 Closed browser for account: {account['name']}")


def validate_environment():
    """Validate all required environment variables are set"""
    required_vars = ["CDL_ACC_JSON", "SPREADSHEET_ID"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Validate JSON format
    try:
        accounts = json.loads(os.environ["CDL_ACC_JSON"])
        if not isinstance(accounts, list):
            raise ValueError("CDL_ACC_JSON must be a list of accounts")
        
        for acc in accounts:
            required_keys = ["name", "username", "password"]
            missing_keys = [key for key in required_keys if key not in acc]
            if missing_keys:
                raise ValueError(f"Account missing keys: {missing_keys}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in CDL_ACC_JSON: {e}")


def main():
    """Main execution function"""
    start_time = datetime.now()
    logging.info("🚀 Starting extraction process")
    
    try:
        # Validate environment
        validate_environment()
        
        # Load configuration
        accounts = json.loads(os.environ["CDL_ACC_JSON"])
        spreadsheet_id = os.environ["SPREADSHEET_ID"]
        worksheet_name = os.environ.get("WORKSHEET_NAME", "dados_cred_commerce")
        
        logging.info(f"📋 Loaded {len(accounts)} accounts")
        
        # Calculate dates
        yesterday = datetime.now() - timedelta(days=1)
        target_day = yesterday.day
        test_day = 1  # Start day
        
        logging.info(f"📅 Filter dates: start={test_day}, end={target_day}")
        
        # Process each account
        all_dfs = []
        successful_accounts = 0
        failed_accounts = 0
        
        for idx, acc in enumerate(accounts, 1):
            logging.info(f"📌 Processing account {idx}/{len(accounts)}: {acc['name']}")
            
            try:
                df_acc = extract_for_account(acc, target_day=target_day, test_day=test_day)
                
                if not df_acc.empty:
                    all_dfs.append(df_acc)
                    successful_accounts += 1
                    logging.info(f"✅ Successfully extracted {len(df_acc)} rows from {acc['name']}")
                else:
                    failed_accounts += 1
                    logging.warning(f"⚠️ No data extracted from {acc['name']}")
                    
            except Exception as e:
                failed_accounts += 1
                logging.error(f"❌ Failed to process {acc['name']}: {e}")
            
            # Small delay between accounts
            if idx < len(accounts):
                time.sleep(3)
        
        # Summary
        logging.info(f"📊 Summary: {successful_accounts} successful, {failed_accounts} failed")
        
        if not all_dfs:
            logging.warning("❌ No data extracted from any account.")
            return
        
        # Combine all data
        final_df = pd.concat(all_dfs, ignore_index=True)
        logging.info(f"📊 Total rows extracted: {len(final_df)}")
        
        # Transform columns for Google Sheets
        column_mapping = {
            "account": "Filial",
            "local": "Cliente",
            "data": "Data Emissão",
            "parcela": "Parcela",
            "valor": "Valor"
        }
        
        final_df = final_df.rename(columns=column_mapping)
        final_df = final_df[["Filial", "Cliente", "Data Emissão", "Parcela", "Valor"]]
        
        # Save to Google Sheets
        append_df_to_sheet(final_df, spreadsheet_id, worksheet_name)
        
        # Save local backup
        backup_file = f"extraction_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
        logging.info(f"💾 Local backup saved to {backup_file}")
        
        # Calculate and log execution time
        elapsed = datetime.now() - start_time
        logging.info(f"✅ Process completed successfully in {elapsed.total_seconds():.2f} seconds")
        
    except Exception as e:
        logging.error(f"❌ Fatal error in main: {e}")
        logging.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
