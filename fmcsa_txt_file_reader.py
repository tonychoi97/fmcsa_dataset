import csv
import pandas as pd
from datetime import datetime, timedelta

def csv_to_dataframe(file_path): # Function that takes a .csv file and converts it to a dataframe.
  with open(file_path, 'r', encoding='windows-1252') as file: # Opening and reading FMCSA text file.
    csv_reader = csv.reader(file) # Stores .csv data as a pandas dataframe.
    data = list(csv_reader) # Converts iterable "csv_reader" into a list named "data".

  df = pd.DataFrame(data[1:], columns = data[0]) # Uses the "data" variable to store the header and values into a pandas dataframe table called "df". data[0] represents the header row, and data[1:] represents rows 1 and beyond.

  return df # Returns table at end of function.

def convert_zip4(df): # Function that converts Zip+4 values to just normal 5 digit zip codes.
  # df.dropna(subset = 'NBR_POWER_UNIT', inplace = True)
  df['PHY_ZIP'] = df['PHY_ZIP'].astype(str) # Converts values to str.
  df['PHY_ZIP'] = df['PHY_ZIP'].str.split('-').str[0] # If value contains "-", then split the value is returned as a list. str[0] will focus on the first value of the list, which is the 5 digit zip code.

def min_fleet_size():
  while True:
    try:
      min_size_input = int(input("Enter the minimum fleet size. Enter 0 if no minimum size: "))
    except ValueError:
      print("Please enter a number.")
    else:
      return min_size_input

def max_fleet_size():
  while True:
    try:
      max_size_input = int(input("Enter the maximum fleet size. Enter 99999 for no limit: "))
    except ValueError:
      print("Please enter a number.")
    else:
      return max_size_input

def zipcode_filter_to_csv(zip_codes, df, file_path, column_names): # Function that takes the dataframe, focusing on the specific state filter, converts it back to a .csv file, and saves it to Google Drive.
  # .to_numeric function converts argument to a numeric type.
  # error='coerce' will set invalid parsing as NaN.
  # .fillna(0) fills NaN values with the desired value.
  df['NBR_POWER_UNIT'] = pd.to_numeric(df['NBR_POWER_UNIT'], errors='coerce').fillna(0).astype(int) # Converts column values, "NBR_POWER_UNIT" to int.
  df['PHY_ZIP'] = df['PHY_ZIP'].astype(str) # Converts column values, "PHY_ZIP" to str.
  zip_codes = list(map(str,zip_codes)) # Converts list, "zip_codes", to str.

  break_loop = True

  while break_loop:
    try:
      min_power_unit = min_fleet_size()
      max_power_unit = max_fleet_size()
    except ValueError:
      print("Please enter a number.")
    else:
      if min_power_unit > max_power_unit:
        print("Minimum fleet size cannot be greater than maximum fleet size.")
      elif min_power_unit <= max_power_unit:
        while True:
          try:
            email_check = int(input("Would you like to only filter for records with email addresses? 1 for yes, 2 for no: "))
          except ValueError:
            print("Please enter a number.")
          else:
            if email_check == 1:
              filtered_states = df[(df['PHY_ZIP'].isin(zip_codes)) & ((df['NBR_POWER_UNIT'] >= min_power_unit) & (df['NBR_POWER_UNIT'] <= max_power_unit)) & (df['EMAIL_ADDRESS'].str.len() > 0)]
              filtered_states.to_csv(file_path, index = False, columns = column_names) # Creates a file in Google Drive.
              print("Your file is ready. Check Google Drive.")
              break_loop = False
              break
            elif email_check == 2:
              filtered_states = df[(df['PHY_ZIP'].isin(zip_codes)) & ((df['NBR_POWER_UNIT'] >= min_power_unit) & (df['NBR_POWER_UNIT'] <= max_power_unit))]
              filtered_states.to_csv(file_path, index = False, columns = column_names) # Creates a file in Google Drive.
              print("Your file is ready. Check Google Drive.")
              break_loop = False
              break
            else:
              print("Out of range. Try again.")

def state_filter_to_csv(states, df, file_path, column_names): # Function that takes the dataframe, focusing on the specific state filter, converts it back to a .csv file, and saves it to Google Drive.
  df['NBR_POWER_UNIT'] = pd.to_numeric(df['NBR_POWER_UNIT'], errors='coerce').fillna(0).astype(int)

  break_loop = True

  while break_loop:
    try:
      min_power_unit = min_fleet_size()
      max_power_unit = max_fleet_size()
    except ValueError:
      print("Please enter a number.")
    else:
      if min_power_unit > max_power_unit:
        print("Minimum fleet size cannot be greater than maximum fleet size.")
      elif min_power_unit <= max_power_unit:
        while True:
          try:
            email_check = int(input("Would you like to only filter for records with email addresses? 1 for yes, 2 for no: "))
          except ValueError:
            print("Please enter a number.")
          else:
            if email_check == 1:
              filtered_states = df[(df['PHY_STATE'].isin(states)) & ((df['NBR_POWER_UNIT'] >= min_power_unit) & (df['NBR_POWER_UNIT'] <= max_power_unit)) & (df['EMAIL_ADDRESS'].str.len() > 0)]
              filtered_states.to_csv(file_path, index = False, columns = column_names) # Creates a file in Google Drive.
              print("Your file is ready. Check Google Drive.")
              break_loop = False
              break
            elif email_check == 2:
              filtered_states = df[(df['PHY_STATE'].isin(states)) & ((df['NBR_POWER_UNIT'] >= min_power_unit) & (df['NBR_POWER_UNIT'] <= max_power_unit))]
              filtered_states.to_csv(file_path, index = False, columns = column_names) # Creates a file in Google Drive.
              print("Your file is ready. Check Google Drive.")
              break_loop = False
              break
            else:
              print("Out of range. Try again.")

while True:
  file_path_name = "FMCSA_CENSUS1_2024Jul.txt"
  data_frame = csv_to_dataframe(file_path_name)
  import_zip_codes = "import zipcodes.csv"
  export_file_path_name = "anaheim 75 mi radius.csv"

  try:
    userInput_method = int(input("What would you like to do?\n" + "Enter 1 to filter by Zip Code.\n" + "Enter 2 to filter by State/Province Abbreviation.\n"))
  except ValueError:
    print("Please enter a number.")
  else:
    if userInput_method == 1:
      # zip_codes = input("Enter your comma separated list of zip codes: ")

      zip_codes_list = []
      with open('import zipcodes.csv', 'r') as f:
          csv_reader = csv.reader(f, delimiter = ',')
          next(csv_reader)
          zip_codes_list = [row[0] for row in csv_reader]

      print(zip_codes_list)
      
      # zip_codes_list = zip_codes.split(',')

      convert_zip4(data_frame)
      zipcode_filter_to_csv(zip_codes_list, data_frame, export_file_path_name, data_frame.columns.values)
      break
    elif userInput_method == 2:
      state_abbreviations = input("Enter your comma separated list of state or province abbreviations.")
      state_abbreviations_list = state_abbreviations.split(',')
      state_filter_to_csv(state_abbreviations_list, data_frame, export_file_path_name, data_frame.columns.values)
      break
    else:
      print("Out of range. Try again.")