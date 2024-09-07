import pandas as pd
import requests
import json # print(json.dumps(data, indent = 4),'\n')
import time
from datetime import date
import csv
import sys

# https://www.calybre.global/post/asynchronous-api-calls-in-python-with-asyncio
# Article on using 'asyncio' to send asynchronous API calls.

def carrier_api_call_option():
    base_url = 'https://mobile.fmcsa.dot.gov/qc/services/carriers/' # Base URL for the API. 
    searchByDOT_url = '?webKey=' # Part of URL used for the general database.

    while True:
        api_key = api_call_key_test(base_url, searchByDOT_url)
        if api_key == False:
            print('Returning to main...\n')
            return
        else:
            break
    
    count = 0
    search_dot_list = get_dot_list_from_csv()
    df1 = pd.DataFrame(search_dot_list, columns = ['DOT NUMBER'])
    df2 = pd.read_csv("active fmcsa records.csv", usecols = ['DOT NUMBER'], header = 0)
    for i in list(df1['DOT NUMBER'].astype(str)):
        duplicate_checker = list(df2['DOT NUMBER'].astype(str)).count(i)
        if duplicate_checker > 0:
            print(f'{i} already exists in the records database.')
            count += 1
            search_dot_list.remove(i)
        else:
            continue
    
    print(f'Found {count} duplicates. Proceeding with API call...\n')

    with open('active fmcsa records.csv', 'a', newline = '', encoding = 'utf-8') as f:
        for i in search_dot_list:
            new_record = {
                "RECORD RETRIEVAL DATE": None,
                "DOT NUMBER": None,
                "DBA NAME": None,
                "LEGAL NAME": None,
                "ALLOWED TO OPERATE": None,
                "CARRIER OPERATION CODE": None,
                "CARRIER OPERATION DESCRIPTION": None,
                "CENSUS TYPE": None,
                "CENSUS TYPE DESCRIPTION": None,
                "CENSUS TYPE ID": None,
                "IS PASSENGER CARRIER?": None,
                "MCS150 OUTDATED?": None,
                "OOS DATE": None,
                "PHY STREET": None,
                "PHY CITY": None,
                "PHY STATE": None,
                "PHY ZIP": None,
                "PHY COUNTRY": None,
                "REVIEW DATE": None,
                "REVIEW TYPE": None,
                "STATUS CODE": None,
                "TOTAL DRIVERS": None,
                "TOTAL POWER UNITS": None
            }
        
            time.sleep(1) # Take 1 second for each record call.
            full_url = f'{base_url}{i}{searchByDOT_url}{api_key}'

            response = requests.get(full_url)
            
            if response.status_code == 200:
                data = response.json()
                if data['content'] == None:
                    print("DOT Number:", i, "does not exist.\n")

                    new_record = {
                        "RECORD RETRIEVAL DATE": date.today(),
                        "DOT NUMBER": i,
                        "DBA NAME": None,
                        "LEGAL NAME": None,
                        "ALLOWED TO OPERATE": None,
                        "CARRIER OPERATION CODE": None,
                        "CARRIER OPERATION DESCRIPTION": None,
                        "CENSUS TYPE": None,
                        "CENSUS TYPE DESCRIPTION": None,
                        "CENSUS TYPE ID": None,
                        "IS PASSENGER CARRIER?": None,
                        "MCS150 OUTDATED?": None,
                        "OOS DATE": None,
                        "PHY STREET": None,
                        "PHY CITY": None,
                        "PHY STATE": None,
                        "PHY ZIP": None,
                        "PHY COUNTRY": None,
                        "REVIEW DATE": None,
                        "REVIEW TYPE": None,
                        "STATUS CODE": None,
                        "TOTAL DRIVERS": None,
                        "TOTAL POWER UNITS": None
                    }

                    csv.writer(f).writerow(new_record.values())

                elif data['content']['carrier']['dbaName'] == None:
                    print("DOT Number:", i, "is inactive.\n")

                    new_record = {
                        "RECORD RETRIEVAL DATE": date.today(),
                        "DOT NUMBER": i,
                        "DBA NAME": None,
                        "LEGAL NAME": None,
                        "ALLOWED TO OPERATE": 'N',
                        "CARRIER OPERATION CODE": None,
                        "CARRIER OPERATION DESCRIPTION": None,
                        "CENSUS TYPE": None,
                        "CENSUS TYPE DESCRIPTION": None,
                        "CENSUS TYPE ID": None,
                        "IS PASSENGER CARRIER?": None,
                        "MCS150 OUTDATED?": None,
                        "OOS DATE": None,
                        "PHY STREET": None,
                        "PHY CITY": None,
                        "PHY STATE": None,
                        "PHY ZIP": None,
                        "PHY COUNTRY": None,
                        "REVIEW DATE": None,
                        "REVIEW TYPE": None,
                        "STATUS CODE": None,
                        "TOTAL DRIVERS": None,
                        "TOTAL POWER UNITS": None
                    }

                    csv.writer(f).writerow(new_record.values())

                else:
                    print("DOT Number:", i, "is", data['content']['carrier']['dbaName'], '\n')
                    new_record= {
                        "RECORD RETRIEVAL DATE": date.today(),
                        "DOT NUMBER": data['content']['carrier']['dotNumber'],
                        "DBA NAME": data['content']['carrier']['dbaName'],
                        "LEGAL NAME": data['content']['carrier']['legalName'],
                        "ALLOWED TO OPERATE": data['content']['carrier']['allowedToOperate'],
                        "CARRIER OPERATION CODE": data['content']['carrier']['carrierOperation']['carrierOperationCode'],
                        "CARRIER OPERATION DESCRIPTION": data['content']['carrier']['carrierOperation']['carrierOperationDesc'],
                        "CENSUS TYPE": data['content']['carrier']['censusTypeId']['censusType'],
                        "CENSUS TYPE DESCRIPTION": data['content']['carrier']['censusTypeId']['censusTypeDesc'],
                        "CENSUS TYPE ID": data['content']['carrier']['censusTypeId']['censusTypeId'],
                        "IS PASSENGER CARRIER?": data['content']['carrier']['isPassengerCarrier'],
                        "MCS150 OUTDATED?": data['content']['carrier']['mcs150Outdated'],
                        "OOS DATE": data['content']['carrier']['oosDate'],
                        "PHY STREET": data['content']['carrier']['phyStreet'],
                        "PHY CITY": data['content']['carrier']['phyCity'],
                        "PHY STATE": data['content']['carrier']['phyState'],
                        "PHY ZIP": data['content']['carrier']['phyZipcode'],
                        "PHY COUNTRY": data['content']['carrier']['phyCountry'],
                        "REVIEW DATE": data['content']['carrier']['reviewDate'],
                        "REVIEW TYPE": data['content']['carrier']['reviewType'],
                        "STATUS CODE": data['content']['carrier']['statusCode'],
                        "TOTAL DRIVERS": data['content']['carrier']['totalDrivers'],
                        "TOTAL POWER UNITS": data['content']['carrier']['totalPowerUnits']
                    }

                    csv.writer(f).writerow(new_record.values())
            else:
                print(f'Error: {response.status_code}\n')
                print('Trying again after 3 seconds...\n')
                time.sleep(3)
                continue

    print("Finished call. Program is now shutting down...")
    sys.exit()

def api_call_key_test(base, web):
    while True:    
        key = input('Please enter a valid API key: ')
        print(f'Testing the key: {key}')
        full_url = f'{base}{1}{web}{key}'
        r = requests.get(full_url)
        if r.status_code == 200:
            print(f'{key} is valid.\n')
            return key
        else:
            print(f'Error: {r.status_code}. {key} is invalid.\n')
            return False

def get_dot_list_from_csv():
    dot = []
    with open('anaheim radius.csv', 'r') as f:
        csv_reader = csv.reader(f, delimiter = ',')
        next(csv_reader)
        dot = [row[0] for row in csv_reader]
    return dot

def main():
    userInput = ''
    while True:
        try:
            userInput = int(input('Please select one of the following options:\n 1. FMCSA Carrier Details by DOT Number\n 2. Cargo Carried Data by DOT Number\n 3. Close Program\n Input: '))
        except ValueError:
            print('Please enter a valid input: ')
        if userInput == 1:
            carrier_api_call_option()
        elif userInput == 2:
            continue
        elif userInput == 3:
            print('Program shutting down...')
            break
        else:
            print('Please enter a valid input: ')

    

if __name__ == '__main__':
    main()