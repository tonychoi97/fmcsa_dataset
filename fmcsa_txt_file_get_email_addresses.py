import csv
import pandas as pd

fmcsa_file_name = 'FMCSA_CENSUS1_2024Jul.txt'
export_file_name = 'Target State Fleets with Emails.csv'
df_dot_numbers = pd.read_csv('Target State Fleets with 25+ Fleet Size.csv')

df_dot_numbers_list = df_dot_numbers['DOT NUMBER'].tolist()

df_dot_numbers_list_int = []

for i in df_dot_numbers_list:
    df_dot_numbers_list_int.append(int(i))

df_dot_numbers['DOT NUMBER'] = df_dot_numbers['DOT NUMBER'].astype(int)

with open(fmcsa_file_name, 'r', encoding='windows-1252') as file: # Opening and reading FMCSA text file.
    csv_reader = csv.reader(file) # Stores .csv data as a pandas dataframe.
    data = list(csv_reader) 

df_fmcsa_records = pd.DataFrame(data[1:], columns = data[0])

df_fmcsa_records['DOT_NUMBER'] = df_fmcsa_records['DOT_NUMBER'].astype(int)

fmcsa_records_extracted = df_fmcsa_records[(df_fmcsa_records['DOT_NUMBER'].isin(df_dot_numbers_list_int))]

fmcsa_records_extracted.to_csv(export_file_name, index = False, header = True)

print("Your file is ready.")

