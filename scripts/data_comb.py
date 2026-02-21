import pandas as pd
import numpy as np
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import json

def get_google_sheets_data(sheet_id, sheet_name):
    """
    Read data from Google Sheets using service account from environment variable
    """
    # Get service account info from environment variable
    service_account_info = json.loads(os.environ.get('GSERVICE_JSON'))
    
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
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
    
    # Convert to DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])
    
    return df

def combine_and_conference_from_gsheets(sheet_id):
    """
    Combine two worksheets from Google Sheets and create a conference report
    """
    print("Reading data from Google Sheets...")
    
    # Read both sheets
    df_credcommerce = get_google_sheets_data(sheet_id, 'dados_cred_commerce')
    df_trier = get_google_sheets_data(sheet_id, 'dados_trier')
    
    print(f"Loaded {len(df_credcommerce)} records from CredCommerce")
    print(f"Loaded {len(df_trier)} records from Trier")
    
    # Standardize column names
    df_credcommerce.columns = ['Filial', 'Cliente', 'Data_Emissao', 'Parcela', 'Valor']
    df_trier.columns = ['Filial', 'Cliente', 'Data_Emissao', 'Parcela', 'Valor']
    
    # Clean and standardize data
    # 1. Convert Filial to integer (trier might have float values as strings)
    df_trier['Filial'] = df_trier['Filial'].astype(float).astype(int)
    df_credcommerce['Filial'] = df_credcommerce['Filial'].astype(int)
    
    # 2. Clean client names
    def clean_client_name(name):
        if pd.isna(name):
            return name
        name = str(name).upper().strip()
        # Remove extra spaces
        name = ' '.join(name.split())
        # Common name variations
        name = name.replace(' JOAO ', ' JOÃO ').replace(' JOAO', ' JOÃO')
        return name
    
    df_credcommerce['Cliente'] = df_credcommerce['Cliente'].apply(clean_client_name)
    df_trier['Cliente'] = df_trier['Cliente'].apply(clean_client_name)
    
    # 3. Extract parcel number from PARCELA X/Y format in trier
    def extract_parcel_number(parcela):
        if pd.isna(parcela):
            return parcela
        parcela = str(parcela)
        if '/' in parcela:
            # Extract number before /
            try:
                return int(parcela.split('/')[0].replace('PARCELA', '').strip())
            except:
                return parcela
        try:
            return int(float(parcela)) if parcela.replace('.','').isdigit() else parcela
        except:
            return parcela
    
    df_trier['Parcela_Num'] = df_trier['Parcela'].apply(extract_parcel_number)
    df_credcommerce['Parcela_Num'] = pd.to_numeric(df_credcommerce['Parcela'], errors='coerce')
    
    # 4. Clean and standardize values
    def clean_value(val):
        if pd.isna(val):
            return val
        val = str(val)
        # Remove R$, commas, etc
        val = val.replace('R$', '').replace('.', '').replace(',', '.').strip()
        try:
            return round(float(val), 2)
        except:
            return val
    
    df_credcommerce['Valor_Num'] = df_credcommerce['Valor'].apply(clean_value)
    df_trier['Valor_Num'] = df_trier['Valor'].apply(clean_value)
    
    # 5. Standardize dates
    df_credcommerce['Data_Emissao'] = pd.to_datetime(df_credcommerce['Data_Emissao'], format='%d/%m/%Y', errors='coerce')
    df_trier['Data_Emissao'] = pd.to_datetime(df_trier['Data_Emissao'], format='%d/%m/%Y', errors='coerce')
    
    # Create a unique key for matching
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
    
    # Create conference dataframe
    all_keys = set(df_credcommerce['Chave']).union(set(df_trier['Chave']))
    
    conference_data = []
    
    for key in all_keys:
        cred_row = df_credcommerce[df_credcommerce['Chave'] == key]
        trier_row = df_trier[df_trier['Chave'] == key]
        
        if len(cred_row) > 0 and len(trier_row) > 0:
            # Match found
            status = '✅ MATCH'
            valor_cred = cred_row['Valor_Num'].iloc[0]
            valor_trier = trier_row['Valor_Num'].iloc[0]
            
            # Check if values match (allow small rounding difference)
            if abs(float(valor_cred) - float(valor_trier)) <= 0.02:
                status_detail = '✓ Valores conferem'
            else:
                status_detail = f'⚠️ VALOR DIFERENTE: CredCommerce={valor_cred:.2f} vs Trier={valor_trier:.2f}'
                status = '⚠️ DISCREPANCIA'
            
            conference_data.append({
                'Filial': cred_row['Filial'].iloc[0],
                'Cliente': cred_row['Cliente'].iloc[0],
                'Data': cred_row['Data_Emissao'].iloc[0].strftime('%d/%m/%Y'),
                'Parcela': cred_row['Parcela_Num'].iloc[0],
                'Valor CredCommerce': f'R$ {valor_cred:.2f}',
                'Valor Trier': f'R$ {valor_trier:.2f}',
                'Status': status,
                'Observação': status_detail
            })
        elif len(cred_row) > 0:
            # Only in CredCommerce
            conference_data.append({
                'Filial': cred_row['Filial'].iloc[0],
                'Cliente': cred_row['Cliente'].iloc[0],
                'Data': cred_row['Data_Emissao'].iloc[0].strftime('%d/%m/%Y'),
                'Parcela': cred_row['Parcela_Num'].iloc[0],
                'Valor CredCommerce': f'R$ {cred_row["Valor_Num"].iloc[0]:.2f}',
                'Valor Trier': 'Não registrado',
                'Status': '❌ FALTA NO TRIER',
                'Observação': 'Registro presente apenas no CredCommerce'
            })
        else:
            # Only in Trier
            conference_data.append({
                'Filial': trier_row['Filial'].iloc[0],
                'Cliente': trier_row['Cliente'].iloc[0],
                'Data': trier_row['Data_Emissao'].iloc[0].strftime('%d/%m/%Y'),
                'Parcela': trier_row['Parcela_Num'].iloc[0],
                'Valor CredCommerce': 'Não registrado',
                'Valor Trier': f'R$ {trier_row["Valor_Num"].iloc[0]:.2f}',
                'Status': '❌ FALTA NO CREDCOMMERCE',
                'Observação': 'Registro presente apenas no Trier'
            })
    
    # Create conference dataframe
    df_conference = pd.DataFrame(conference_data)
    
    # Sort by date, then filial, then client
    df_conference['Data_sort'] = pd.to_datetime(df_conference['Data'], format='%d/%m/%Y')
    df_conference = df_conference.sort_values(['Data_sort', 'Filial', 'Cliente', 'Parcela'])
    df_conference = df_conference.drop('Data_sort', axis=1)
    
    # Create summary statistics
    summary = {
        'Total de Registros': len(df_conference),
        'Matches Perfeitos': len(df_conference[df_conference['Status'] == '✅ MATCH']),
        'Discrepâncias de Valor': len(df_conference[df_conference['Status'] == '⚠️ DISCREPANCIA']),
        'Falta no Trier': len(df_conference[df_conference['Status'] == '❌ FALTA NO TRIER']),
        'Falta no CredCommerce': len(df_conference[df_conference['Status'] == '❌ FALTA NO CREDCOMMERCE'])
    }
    
    # Daily summary
    df_conference['Data_dt'] = pd.to_datetime(df_conference['Data'], format='%d/%m/%Y')
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
    
    return df_conference, pd.DataFrame([summary]), daily_summary

def save_to_google_sheets(df_conference, df_summary, df_daily, sheet_id):
    """
    Save the conference results back to Google Sheets
    """
    print("Saving results to Google Sheets...")
    
    try:
        # Try Colab authentication first
        auth.authenticate_user()
        from google.auth import default
        creds, _ = default()
        gc = gspread.authorize(creds)
    except:
        # Fall back to service account
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(
            'service-account.json', scopes=scope
        )
        gc = gspread.authorize(creds)
    
    # Open the spreadsheet
    workbook = gc.open_by_key(sheet_id)
    
    # Create or update conference sheet
    try:
        # Try to get existing sheet
        worksheet = workbook.worksheet('Conferência_Diária')
        workbook.del_worksheet(worksheet)
    except:
        pass
    
    # Create new sheet
    worksheet = workbook.add_worksheet(title='Conferência_Diária', rows=1000, cols=20)
    
    # Update with conference data
    worksheet.update([df_conference.columns.values.tolist()] + df_conference.values.tolist())
    
    # Create summary sheet
    try:
        worksheet_summary = workbook.worksheet('Resumo')
        workbook.del_worksheet(worksheet_summary)
    except:
        pass
    
    worksheet_summary = workbook.add_worksheet(title='Resumo', rows=100, cols=10)
    
    # Write summary
    summary_data = [
        ['MÉTRICA', 'VALOR'],
        ['Total de Registros', df_summary['Total de Registros'].iloc[0]],
        ['Matches Perfeitos', df_summary['Matches Perfeitos'].iloc[0]],
        ['Discrepâncias de Valor', df_summary['Discrepâncias de Valor'].iloc[0]],
        ['Falta no Trier', df_summary['Falta no Trier'].iloc[0]],
        ['Falta no CredCommerce', df_summary['Falta no CredCommerce'].iloc[0]]
    ]
    worksheet_summary.update(summary_data)
    
    # Create daily summary sheet
    try:
        worksheet_daily = workbook.worksheet('Resumo_Diário')
        workbook.del_worksheet(worksheet_daily)
    except:
        pass
    
    worksheet_daily = workbook.add_worksheet(title='Resumo_Diário', rows=100, cols=10)
    worksheet_daily.update([df_daily.columns.values.tolist()] + df_daily.values.tolist())
    
    print("Results saved successfully to Google Sheets!")

# Main execution
if __name__ == "__main__":
    SHEET_ID = os.getenv("SPREADSHEET_ID")
    
    print(f"Starting conference for Google Sheet: {SHEET_ID}")
    
    try:
        # Run the conference
        df_conference, df_summary, df_daily = combine_and_conference_from_gsheets(SHEET_ID)
        
        # Save results back to Google Sheets
        save_to_google_sheets(df_conference, df_summary, df_daily, SHEET_ID)
        
        print("\n" + "="*50)
        print("CONFERENCE COMPLETED SUCCESSFULLY!")
        print("="*50)
        print("\nSUMMARY:")
        print(df_summary.to_string(index=False))
        print("\nDAILY SUMMARY:")
        print(df_daily.to_string(index=False))
        
        # Highlight issues
        issues = df_conference[df_conference['Status'] != '✅ MATCH']
        if len(issues) > 0:
            print(f"\n⚠️  ATTENTION: Found {len(issues)} issues that need review!")
            print("\nFirst 5 issues:")
            print(issues[['Data', 'Filial', 'Cliente', 'Status', 'Observação']].head().to_string(index=False))
        else:
            print("\n✅ All records match perfectly!")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Make sure your SHEET_ID is correct")
        print("2. Ensure the sheet is shared with your service account email")
        print("3. Check that sheet names are exactly: 'dados_cred_commerce' and 'dados_trier'")
