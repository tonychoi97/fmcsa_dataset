import pandas as pd
import numpy as np
import requests
import sqlalchemy as sa
import json
import time
import csv

# engine = sa.create_engine('postgresql://user:pass@hot:port/db')

fmcsaWebKey = '56766bef6b098253712655a59b0aaf26902d2eea' # Personal API Key for FMCSA.
base_url = 'https://mobile.fmcsa.dot.gov/qc/services/carriers/' # Base URL for the API.
cargoCarried_url = '/cargo-carried?webKey=' # Part of URL used for the cargo carried database. 
searchByDOT_url = '?webKey=' # Part of URL used for the general database.

table_cargo = {
    'DOT Number': [],
    'General Freight': [],
    'Household Goods': [],
    'Metal; Sheets, Coils, Rolls': [],
    'Motor Vehicles': [],
    'Drive/Tow away': [],
    'Logs, Poles, Beams, Lumber': [],
    'Building Materials': [],
    'Mobile Homes': [],
    'Machinery, Large Objects': [],
    'Fresh Produce': [],
    'Liquids/Gases': [],
    'Intermodal Cont.': [],
    'Passengers': [],
    'Oilfield Equipment': [],
    'Livestock': [],
    'Grain, Feed, Hay': [],
    'Coal/Coke': [],
    'Meat': [],
    'Garbage/Refuse': [],
    'US Mail': [],
    'Chemicals': [],
    'Commodities Dry Bulk': [],
    'Refrigerated Dry Food': [],
    'Beverages': [],
    'Paper Products': [],
    'Utilities': [],
    'Agricultural/Farm Supplies': [],
    'Construction': [],
    'Water Well': []
}

search_dot_list = [1000000,1000079,1612089,1612100,1612128,1612139,1612141]
# with open('pge fleet research project.csv', 'r') as f:
#     search_dot_list = [row[0] for row in csv.reader(f)]

df2 = pd.DataFrame(table_cargo)

for i in search_dot_list:

    time.sleep(.5)

    full_url = f'{base_url}{i}{cargoCarried_url}{fmcsaWebKey}'

    response = requests.get(full_url)
    if response.status_code == 200:
        data = response.json()

        if not data['content']:
            print("DOT Number:", i, "list is empty.")
        elif data['content'] == None:
            print("DOT Number:", i, "does not exist.")
        else:
            print("DOT Number:", i)
            
            new_cargo_entry = {
                'DOT Number': None,
                'General Freight': None,
                'Household Goods': None,
                'Metal; Sheets, Coils, Rolls': None,
                'Motor Vehicles': None,
                'Drive/Tow away': None,
                'Logs, Poles, Beams, Lumber': None,
                'Building Materials': None,
                'Mobile Homes': None,
                'Machinery, Large Objects': None,
                'Fresh Produce': None,
                'Liquids/Gases': None,
                'Intermodal Cont.': None,
                'Passengers': None,
                'Oilfield Equipment': None,
                'Livestock': None,
                'Grain, Feed, Hay': None,
                'Coal/Coke': None,
                'Meat': None,
                'Garbage/Refuse': None,
                'US Mail': None,
                'Chemicals': None,
                'Commodities Dry Bulk': None,
                'Refrigerated Dry Food': None,
                'Beverages': None,
                'Paper Products': None,
                'Utilities': None,
                'Agricultural/Farm Supplies': None,
                'Construction': None,
                'Water Well': None
            }
            
            cargoList = []
            for i in range(len(data['content'])):
                cargoList.append(data['content'][i]['cargoClassDesc'])
            print(cargoList)

            assignToDict = []
            for key in new_cargo_entry.keys():
                assignedChecker = False
                if key == 'DOT Number':
                    assignToDict.append(data['content'][0]['id']['dotNumber'])
                else:
                    for value in cargoList:
                        if key == value:
                            assignToDict.append('Y')
                            assignedChecker = True
                            break
                    if assignedChecker == False:      
                        assignToDict.append('N')
                        
            newEntry = dict(zip(new_cargo_entry,assignToDict))
            df2 = pd.concat([df2, pd.DataFrame([newEntry])], ignore_index = True)
    else:
        print(f'Error: {response.status_code}', '\n')
        print(f'Trying again after 5 seconds.', '\n')
        time.sleep(5)
        continue

print(df2)
df2.to_csv('testexport.csv', index = False)