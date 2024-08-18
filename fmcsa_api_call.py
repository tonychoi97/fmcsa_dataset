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

# table_dot = {
#     "DOT NUMBER": [],
#     "DBA NAME": [],
#     "LEGAL NAME": [],
#     "CARRIER OPERATION CODE": [],
#     "CARRIER OPERATION DESCRIPTION": [],
#     "MCS150 OUTDATED?": [],
#     "PHY STREET": [],
#     "PHY CITY": [],
#     "PHY STATE": [],
#     "PHY ZIP": [],
#     "PHY COUNTRY": [],
#     "TOTAL DRIVERS": [],
#     "TOTAL POWER UNITS": []
# }

# df1 = pd.DataFrame(table_dot)

# for i in range(1000000, 1000010):
#     full_url = f'{base_url}{i}{searchByDOT_url}{fmcsaWebKey}'

#     response = requests.get(full_url)
#     if response.status_code == 200:
#         data = response.json()
#         if data['content'] == None:
#             print("DOT Number:", i, "does not exist.")
        
#         elif data['content']['carrier']['dbaName'] == None:
#             print("DOT Number:", i, "is inactive.")
#         else:
#             print("DOT Number:", i, "is", data['content']['carrier']['dbaName'])

#             new_row = {
#                 "DOT NUMBER": [data['content']['carrier']['dotNumber']],
#                 "DBA NAME": [data['content']['carrier']['dbaName']],
#                 "LEGAL NAME": [data['content']['carrier']['legalName']],
#                 "CARRIER OPERATION CODE": [data['content']['carrier']['carrierOperation']['carrierOperationCode']],
#                 "CARRIER OPERATION DESCRIPTION": [data['content']['carrier']['carrierOperation']['carrierOperationDesc']],
#                 "MCS150 OUTDATED?": [data['content']['carrier']['mcs150Outdated']],
#                 "PHY STREET": [data['content']['carrier']['phyStreet']],
#                 "PHY CITY": [data['content']['carrier']['phyCity']],
#                 "PHY STATE": [data['content']['carrier']['phyState']],
#                 "PHY ZIP": [data['content']['carrier']['phyZipcode']],
#                 "PHY COUNTRY": [data['content']['carrier']['phyCountry']],
#                 "TOTAL DRIVERS": [data['content']['carrier']['totalDrivers']],
#                 "TOTAL POWER UNITS": [data['content']['carrier']['totalPowerUnits']]
#             }

#             df1 = pd.concat([df1, pd.DataFrame([new_row])], ignore_index = True)
#     else:
#         print(f'Error: {response.status_code}', '\n')

# print(df1)

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

# for i in table_cargo.keys():
#     print(i)

search_dot_list = []
with open('pge fleet research project.csv', 'r') as f:
    search_dot_list = [row[0] for row in csv.reader(f)]

df2 = pd.DataFrame(table_cargo)

for i in search_dot_list:

    time.sleep(3)

    full_url = f'{base_url}{i}{cargoCarried_url}{fmcsaWebKey}'

    response = requests.get(full_url)
    if response.status_code == 200:
        data = response.json()
        # print(json.dumps(data, indent = 4))

        # print(type(data['content']))

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

            # assignToDict = []
            # value = ''
            # for i in cargoList:
            #     j = value
            #     for j in new_cargo_entry.keys():
            #         if i == 'DOT Number':
            #             assignToDict.append(data['content'][0]['id']['dotNumber'])
            #         elif i == j:
            #             assignToDict.append(True)
            #             value = j
            #             break
            #         else:
            #             assignToDict.append(False)

            # newEntry = dict(zip(new_cargo_entry, assignToDict))

            # df2 = pd.concat([df2, pd.DataFrame([newEntry])], ignore_index = True)

    else:
        print(f'Error: {response.status_code}', '\n')

print(df2)
df2.to_csv('pge_fleet_research_export.csv', index = False)
# full_url = f'{base_url}{1000000}{cargoCarried_url}{fmcsaWebKey}'
# response = requests.get(full_url)
# data = response.json()
# print(json.dumps(data, indent = 4))

# print(json.dumps(data, indent = 4)) - code for "pretty printing" json records
