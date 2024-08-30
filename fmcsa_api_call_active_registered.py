import pandas as pd
import numpy as np
import requests
import sqlalchemy as sa
import json
import time
import csv

# https://www.calybre.global/post/asynchronous-api-calls-in-python-with-asyncio

fmcsaWebKey = '56766bef6b098253712655a59b0aaf26902d2eea' # Personal API Key for FMCSA.
base_url = 'https://mobile.fmcsa.dot.gov/qc/services/carriers/' # Base URL for the API.
cargoCarried_url = '/cargo-carried?webKey=' # Part of URL used for the cargo carried database. 
searchByDOT_url = '?webKey=' # Part of URL used for the general database.

table_dot = {
    "DOT NUMBER": [],
    "DBA NAME": [],
    "LEGAL NAME": [],
    "CARRIER OPERATION CODE": [],
    "CARRIER OPERATION DESCRIPTION": [],
    "MCS150 OUTDATED?": [],
    "PHY STREET": [],
    "PHY CITY": [],
    "PHY STATE": [],
    "PHY ZIP": [],
    "PHY COUNTRY": [],
    "TOTAL DRIVERS": [],
    "TOTAL POWER UNITS": []
}

search_dot_list = [1000000,1000079,1612089,1612100,1612128,1612139,1612141]
# with open('list of pge dot.csv', 'r') as f:
#     search_dot_list = [row[0] for row in csv.reader(f)]

df1 = pd.DataFrame(table_dot)
    
for i in search_dot_list:
    time.sleep(.5)

    full_url = f'{base_url}{i}{searchByDOT_url}{fmcsaWebKey}'

    response = requests.get(full_url)
    if response.status_code == 200:
        data = response.json()
        if data['content'] == None:
            print("DOT Number:", i, "does not exist.")
        elif data['content']['carrier']['dbaName'] == None:
            print("DOT Number:", i, "is inactive.")
        else:
            print("DOT Number:", i, "is", data['content']['carrier']['dbaName'])
            print(json.dumps(data, indent = 4))
            new_row = {
                "DOT NUMBER": [data['content']['carrier']['dotNumber']],
                "DBA NAME": [data['content']['carrier']['dbaName']],
                "LEGAL NAME": [data['content']['carrier']['legalName']],
                "CARRIER OPERATION CODE": [data['content']['carrier']['carrierOperation']['carrierOperationCode']],
                "CARRIER OPERATION DESCRIPTION": [data['content']['carrier']['carrierOperation']['carrierOperationDesc']],
                "MCS150 OUTDATED?": [data['content']['carrier']['mcs150Outdated']],
                "PHY STREET": [data['content']['carrier']['phyStreet']],
                "PHY CITY": [data['content']['carrier']['phyCity']],
                "PHY STATE": [data['content']['carrier']['phyState']],
                "PHY ZIP": [data['content']['carrier']['phyZipcode']],
                "PHY COUNTRY": [data['content']['carrier']['phyCountry']],
                "TOTAL DRIVERS": [data['content']['carrier']['totalDrivers']],
                "TOTAL POWER UNITS": [data['content']['carrier']['totalPowerUnits']]
            }

            df1 = pd.concat([df1, pd.DataFrame([new_row])], ignore_index = True)
    else:
        print(f'Error: {response.status_code}', '\n')
        print(f'Trying again after 5 seconds.', '\n')
        time.sleep(5)
        continue

print(df1)
df1.to_csv('testexport.csv', index = False)