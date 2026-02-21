import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from datetime import datetime

def get_google_sheets_data(sheet_id, sheet_name):
    """
    Read data from Google Sheets using service account from environment variable
    """
    print(f"  Reading sheet: {sheet_name}...")
    
    # Get service account info from environment variable
    service_account_info = json.loads(os.environ.get('GSERVICE_JSON'))
    
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive',
             'https://www.googleapis.com/auth/spreadsheets']
    
    # Create credentials using the service account info
    creds = Credentials.from_service_account_info(
        service_account_info, 
        scopes=scope
    )
    
    # Authorize with gspread
    gc = gspread.authorize(creds)
    
    # Open the spreadsheet
    workbook = gc.open_by_key(sheet_id)
    
    # Get the specific worksheet
    worksheet = workbook.worksheet(sheet_name)
    
    # Get all values
    data = worksheet.get_all_values()
    
    print(f"    Raw data: {len(data)} rows found")
    
    # Convert to DataFrame (first row as headers, rest as data)
    if len(data) > 1:  # At least header + 1 data row
        df = pd.DataFrame(data[1:], columns=data[0])
        print(f"    DataFrame created with {len(df)} records")
        return df
    elif len(data) == 1:  # Only headers, no data
        print(f"    Only headers found, creating empty DataFrame")
        return pd.DataFrame(columns=data[0])
    else:  # Empty sheet
        print(f"    Empty sheet, creating empty DataFrame with default columns")
        return pd.DataFrame(columns=['Filial', 'Cliente', 'Data_Emissao', 'Parcela', 'Valor'])

def combine_and_conference_from_gsheets(sheet_id):
    """
    Combine two worksheets from Google Sheets and create a conference report
    """
    print("\n📊 Reading data from Google Sheets...")
    
    # Read both sheets
    try:
        df_credcommerce = get_google_sheets_data(sheet_id, 'dados_cred_commerce')
        df_trier = get_google_sheets_data(sheet_id, 'dados_trier')
        
        print(f"\n✅ Loaded {len(df_credcommerce)} records from CredCommerce")
        print(f"✅ Loaded {len(df_trier)} records from Trier")
        
        # Debug: Print first few rows to see the data
        if len(df_credcommerce) > 0:
            print("\n📋 First 2 rows from CredCommerce:")
            print(df_credcommerce.head(2).to_string())
        
        if len(df_trier) > 0:
            print("\n📋 First 2 rows from Trier:")
            print(df_trier.head(2).to_string())
            
    except Exception as e:
        print(f"❌ Error reading sheets: {str(e)}")
        raise
    
    # Check if DataFrames are empty
    if len(df_credcommerce) == 0 and len(df_trier) == 0:
        print("❌ Both sheets are empty!")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Standardize column names
    df_credcommerce.columns = ['Filial', 'Cliente', 'Data_Emissao', 'Parcela', 'Valor']
    df_trier.columns = ['Filial', 'Cliente', 'Data_Emissao', 'Parcela', 'Valor']
    
    # Clean and standardize data
    # 1. Convert Filial to integer
    try:
        df_trier['Filial'] = pd.to_numeric(df_trier['Filial'], errors='coerce').fillna(0).astype(int)
        df_credcommerce['Filial'] = pd.to_numeric(df_credcommerce['Filial'], errors='coerce').fillna(0).astype(int)
    except Exception as e:
        print(f"⚠️ Warning: Error converting Filial: {e}")
        df_trier['Filial'] = 0
        df_credcommerce['Filial'] = 0
    
    # 2. Clean client names
    def clean_client_name(name):
        if pd.isna(name):
            return "NOME NAO INFORMADO"
        name = str(name).upper().strip()
        # Remove extra spaces
        name = ' '.join(name.split())
        # Common name variations
        name = name.replace(' JOAO ', ' JOÃO ').replace(' JOAO', ' JOÃO')
        return name
    
    df_credcommerce['Cliente'] = df_credcommerce['Cliente'].apply(clean_client_name)
    df_trier['Cliente'] = df_trier['Cliente'].apply(clean_client_name)
    
    # 3. Extract parcel number
    def extract_parcel_number(parcela):
        if pd.isna(parcela):
            return 0
        parcela = str(parcela)
        if '/' in parcela:
            # Extract number before /
            try:
                return int(parcela.split('/')[0].replace('PARCELA', '').strip())
            except:
                return 0
        try:
            return int(float(parcela)) if parcela.replace('.','').replace('-','').isdigit() else 0
        except:
            return 0
    
    df_trier['Parcela_Num'] = df_trier['Parcela'].apply(extract_parcel_number)
    df_credcommerce['Parcela_Num'] = pd.to_numeric(df_credcommerce['Parcela'], errors='coerce').fillna(0).astype(int)
    
    # 4. Clean and standardize values
    def clean_value(val):
        if pd.isna(val):
            return 0.0
        val = str(val)
        # Remove R$, commas, etc
        val = val.replace('R$', '').replace('.', '').replace(',', '.').strip()
        try:
            return round(float(val), 2)
        except:
            return 0.0
    
    df_credcommerce['Valor_Num'] = df_credcommerce['Valor'].apply(clean_value)
    df_trier['Valor_Num'] = df_trier['Valor'].apply(clean_value)
    
    # 5. Standardize dates
    try:
        df_credcommerce['Data_Emissao'] = pd.to_datetime(df_credcommerce['Data_Emissao'], format='%d/%m/%Y', errors='coerce')
        df_trier['Data_Emissao'] = pd.to_datetime(df_trier['Data_Emissao'], format='%d/%m/%Y', errors='coerce')
    except Exception as e:
        print(f"⚠️ Warning: Error converting dates: {e}")
        df_credcommerce['Data_Emissao'] = pd.NaT
        df_trier['Data_Emissao'] = pd.NaT
    
    # Remove rows with invalid dates (but keep them with a default date)
    default_date = pd.Timestamp.now().normalize()
    df_credcommerce['Data_Emissao'] = df_credcommerce['Data_Emissao'].fillna(default_date)
    df_trier['Data_Emissao'] = df_trier['Data_Emissao'].fillna(default_date)
    
    print("\n🔄 Creating match keys...")
    
    # Create a unique key for matching
    try:
        df_credcommerce['Chave'] = (
            df_credcommerce['Filial'].astype(str) + '_' + 
            df_credcommerce['Cliente'] + '_' + 
            df_credcommerce['Data_Emissao'].dt.strftime('%Y-%m-%d') + '_' + 
            df_credcommerce['Parcela_Num'].astype(str)
        )
        
        df_trier['Chave'] = (
            df_trier['Filial'].astype(str) + '_' + 
            df_trier['Cliente'] + '_' + 
            df_trier['Data_Emissao'].dt.strftime('%Y-%m-%d') + '_' + 
            df_trier['Parcela_Num'].astype(str)
        )
    except Exception as e:
        print(f"❌ Error creating keys: {e}")
        raise
    
    print("🔄 Comparing records...")
    
    # Create conference dataframe
    all_keys = set(df_credcommerce['Chave']).union(set(df_trier['Chave']))
    print(f"   Total unique keys: {len(all_keys)}")
    
    conference_data = []
    
    for i, key in enumerate(all_keys):
        if i % 20 == 0 and i > 0:
            print(f"   Processed {i}/{len(all_keys)} keys...")
            
        cred_row = df_credcommerce[df_credcommerce['Chave'] == key]
        trier_row = df_trier[df_trier['Chave'] == key]
        
        if len(cred_row) > 0 and len(trier_row) > 0:
            # Match found
            status = '✅ MATCH'
            valor_cred = float(cred_row['Valor_Num'].iloc[0])
            valor_trier = float(trier_row['Valor_Num'].iloc[0])
            
            # Check if values match (allow small rounding difference)
            if abs(valor_cred - valor_trier) <= 0.02:
                status_detail = '✓ Valores conferem'
            else:
                status_detail = f'⚠️ VALOR DIFERENTE: CredCommerce={valor_cred:.2f} vs Trier={valor_trier:.2f}'
                status = '⚠️ DISCREPANCIA'
            
            conference_data.append({
                'Filial': int(cred_row['Filial'].iloc[0]),
                'Cliente': cred_row['Cliente'].iloc[0],
                'Data': cred_row['Data_Emissao'].iloc[0].strftime('%d/%m/%Y'),
                'Parcela': int(cred_row['Parcela_Num'].iloc[0]),
                'Valor CredCommerce': f'R$ {valor_cred:.2f}',
                'Valor Trier': f'R$ {valor_trier:.2f}',
                'Status': status,
                'Observação': status_detail
            })
        elif len(cred_row) > 0:
            # Only in CredCommerce
            conference_data.append({
                'Filial': int(cred_row['Filial'].iloc[0]),
                'Cliente': cred_row['Cliente'].iloc[0],
                'Data': cred_row['Data_Emissao'].iloc[0].strftime('%d/%m/%Y'),
                'Parcela': int(cred_row['Parcela_Num'].iloc[0]),
                'Valor CredCommerce': f'R$ {float(cred_row["Valor_Num"].iloc[0]):.2f}',
                'Valor Trier': 'Não registrado',
                'Status': '❌ FALTA NO TRIER',
                'Observação': 'Registro presente apenas no CredCommerce'
            })
        else:
            # Only in Trier
            conference_data.append({
                'Filial': int(trier_row['Filial'].iloc[0]),
                'Cliente': trier_row['Cliente'].iloc[0],
                'Data': trier_row['Data_Emissao'].iloc[0].strftime('%d/%m/%Y'),
                'Parcela': int(trier_row['Parcela_Num'].iloc[0]),
                'Valor CredCommerce': 'Não registrado',
                'Valor Trier': f'R$ {float(trier_row["Valor_Num"].iloc[0]):.2f}',
                'Status': '❌ FALTA NO CREDCOMMERCE',
                'Observação': 'Registro presente apenas no Trier'
            })
    
    print(f"   Processed all {len(all_keys)} keys")
    
    # Create conference dataframe
    df_conference = pd.DataFrame(conference_data)
    
    if len(df_conference) == 0:
        print("⚠️ No conference data generated!")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Sort by date, then filial, then client
    try:
        df_conference['Data_sort'] = pd.to_datetime(df_conference['Data'], format='%d/%m/%Y', errors='coerce')
        df_conference = df_conference.sort_values(['Data_sort', 'Filial', 'Cliente', 'Parcela'])
        df_conference = df_conference.drop('Data_sort', axis=1)
    except Exception as e:
        print(f"⚠️ Warning: Error sorting data: {e}")
    
    # Create summary statistics
    summary = {
        'Total de Registros': len(df_conference),
        'Matches Perfeitos': len(df_conference[df_conference['Status'] == '✅ MATCH']),
        'Discrepâncias de Valor': len(df_conference[df_conference['Status'] == '⚠️ DISCREPANCIA']),
        'Falta no Trier': len(df_conference[df_conference['Status'] == '❌ FALTA NO TRIER']),
        'Falta no CredCommerce': len(df_conference[df_conference['Status'] == '❌ FALTA NO CREDCOMMERCE'])
    }
    
    # Daily summary
    try:
        df_conference['Data_dt'] = pd.to_datetime(df_conference['Data'], format='%d/%m/%Y', errors='coerce')
        daily_summary = df_conference.groupby(df_conference['Data_dt'].dt.date).agg({
            'Status': [
                ('Total', 'count'),
                ('Matches', lambda x: sum(x == '✅ MATCH')),
                ('Discrepâncias', lambda x: sum(x == '⚠️ DISCREPANCIA')),
                ('Falta_Trier', lambda x: sum(x == '❌ FALTA NO TRIER')),
                ('Falta_CredCommerce', lambda x: sum(x == '❌ FALTA NO CREDCOMMERCE'))
            ]
        }).round(0)
        
        daily_summary.columns = ['Total', 'Matches', 'Discrepâncias', 'Falta no Trier', 'Falta no CredCommerce']
        daily_summary = daily_summary.reset_index()
        daily_summary['Data'] = pd.to_datetime(daily_summary['index']).dt.strftime('%d/%m/%Y')
        daily_summary = daily_summary.drop('index', axis=1)
    except Exception as e:
        print(f"⚠️ Warning: Error creating daily summary: {e}")
        daily_summary = pd.DataFrame(columns=['Data', 'Total', 'Matches', 'Discrepâncias', 'Falta no Trier', 'Falta no CredCommerce'])
    
    return df_conference, pd.DataFrame([summary]), daily_summary

def save_to_google_sheets(df_conference, df_summary, df_daily, sheet_id):
    """
    Save the conference results back to Google Sheets
    """
    print("\n💾 Saving results to Google Sheets...")
    
    # Get service account info from environment variable
    service_account_info = json.loads(os.environ.get('GOOGLE_SHEETS_SERVICE_ACCOUNT'))
    
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive',
             'https://www.googleapis.com/auth/spreadsheets']
    
    # Create credentials
    creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
    
    # Authorize
    gc = gspread.authorize(creds)
    
    # Open the spreadsheet
    workbook = gc.open_by_key(sheet_id)
    
    # Helper function to create or replace worksheet
    def create_or_replace_worksheet(title, rows=1000, cols=20):
        try:
            worksheet = workbook.worksheet(title)
            workbook.del_worksheet(worksheet)
            print(f"   Replaced existing sheet: {title}")
        except:
            print(f"   Creating new sheet: {title}")
        return workbook.add_worksheet(title=title, rows=rows, cols=cols)
    
    timestamp = datetime.now().strftime('%d/%m/%Y %H:%M')
    
    # Save detailed conference data
    if len(df_conference) > 0:
        worksheet_detail = create_or_replace_worksheet('Conferência_Diária')
        worksheet_detail.update([df_conference.columns.values.tolist()] + df_conference.values.tolist())
        print(f"   Saved {len(df_conference)} records to Conferência_Diária")
    
    # Save summary
    if len(df_summary) > 0:
        worksheet_summary = create_or_replace_worksheet('Resumo', rows=100, cols=10)
        summary_data = [
            ['MÉTRICA', 'VALOR'],
            ['Total de Registros', df_summary['Total de Registros'].iloc[0]],
            ['Matches Perfeitos', df_summary['Matches Perfeitos'].iloc[0]],
            ['Discrepâncias de Valor', df_summary['Discrepâncias de Valor'].iloc[0]],
            ['Falta no Trier', df_summary['Falta no Trier'].iloc[0]],
            ['Falta no CredCommerce', df_summary['Falta no CredCommerce'].iloc[0]],
            ['Data da Conferência', timestamp]
        ]
        worksheet_summary.update(summary_data)
    
    # Save daily summary
    if len(df_daily) > 0:
        worksheet_daily = create_or_replace_worksheet('Resumo_Diário', rows=100, cols=10)
        worksheet_daily.update([df_daily.columns.values.tolist()] + df_daily.values.tolist())
    
    # Create a sheet with only issues
    if len(df_conference) > 0:
        issues = df_conference[df_conference['Status'] != '✅ MATCH']
        if len(issues) > 0:
            worksheet_issues = create_or_replace_worksheet('Problemas', rows=100, cols=10)
            worksheet_issues.update([issues.columns.values.tolist()] + issues.values.tolist())
            print(f"   Saved {len(issues)} issues to Problemas sheet")
    
    print(f"\n✅ Results saved successfully to Google Sheet!")
    print(f"📝 Created sheets: Conferência_Diária, Resumo, Resumo_Diário" + (", Problemas" if len(df_conference) > 0 and len(issues) > 0 else ""))

# Main execution
if __name__ == "__main__":
    # Get sheet ID from environment variable
    SHEET_ID = os.environ.get('SPREADSHEET_ID')
    
    if not SHEET_ID:
        print("❌ Error: GOOGLE_SHEET_ID environment variable not set")
        exit(1)
    
    print("="*60)
    print("📊 GOOGLE SHEETS DAILY CONFERENCE")
    print("="*60)
    print(f"🆔 Sheet ID: {SHEET_ID}")
    print(f"📅 Date: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("="*60)
    
    try:
        # Run the conference
        df_conference, df_summary, df_daily = combine_and_conference_from_gsheets(SHEET_ID)
        
        if len(df_conference) == 0:
            print("\n⚠️ No data to process. Both sheets might be empty.")
            exit(0)
        
        # Save results back to Google Sheets
        save_to_google_sheets(df_conference, df_summary, df_daily, SHEET_ID)
        
        print("\n" + "="*60)
        print("✅ CONFERENCE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\n📊 SUMMARY:")
        print(df_summary.to_string(index=False))
        
        if len(df_daily) > 0:
            print("\n📅 DAILY SUMMARY:")
            print(df_daily.to_string(index=False))
        
        # Highlight issues
        issues = df_conference[df_conference['Status'] != '✅ MATCH']
        if len(issues) > 0:
            print(f"\n⚠️  ATTENTION: Found {len(issues)} issues that need review!")
            print("\nFirst 5 issues:")
            print(issues[['Data', 'Filial', 'Cliente', 'Status', 'Observação']].head(5).to_string(index=False))
        else:
            print("\n✅ All records match perfectly!")
            
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        print("\nTroubleshooting tips:")
        print("1. Check if GOOGLE_SHEETS_SERVICE_ACCOUNT secret is set correctly")
        print("2. Verify GOOGLE_SHEET_ID is correct")
        print("3. Ensure the sheet is shared with the service account email")
        print("4. Check that sheet names are exactly: 'dados_cred_commerce' and 'dados_trier'")
        print("5. Verify the data in the sheets has the expected columns")
        exit(1)
