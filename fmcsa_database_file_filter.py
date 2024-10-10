import csv
import pandas as pd

df_db = pd.read_csv('active fmcsa records updated 9.27.2024.csv')
df_zip = pd.read_csv('Anaheim 75mi Zip Code Radius List.csv')

zip_list = df_zip['Zip Code'].tolist()

active_fleets = df_db[(df_db['RECORD EXISTS'] == 'Y')]

# For Anaheim Zip Code Contacts
# active_fleets['PHY ZIP'] = active_fleets['PHY ZIP'].astype(str)
# active_fleets['PHY ZIP'] = active_fleets['PHY ZIP'].str.split('-').str[0]

# active_fleets['PHY ZIP'] = active_fleets['PHY ZIP'].astype(int)
# fmcsa_records_extracted = active_fleets[((active_fleets['PHY ZIP'].isin(zip_list)) & (active_fleets['TOTAL POWER UNITS'] >= 25) & (active_fleets['ALLOWED TO OPERATE'] == 'Y'))]

# For Target States
target_states = ['MA', 'NJ', 'NY', 'NM', 'RI', 'OR', 'VT', 'WA', 'CO', 'MD']
fmcsa_records_extracted = active_fleets[((active_fleets['PHY STATE'].isin(target_states)) & (active_fleets['TOTAL POWER UNITS'] >= 25) & (active_fleets['ALLOWED TO OPERATE'] == 'Y'))]

print(fmcsa_records_extracted)
fmcsa_records_extracted.to_csv('Target State Fleets with 25+ Fleet Size.csv', index = False, header = True)
print("Your file is ready")