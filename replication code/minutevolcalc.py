import pandas as pd
import numpy as np
import os

# Function to calculate returns
def calculate_returns(df):
    print("removing")
    df = df[df['MMID'] != 'WEDN']
    df = df[df['MMID'] != 'WEDO']
    df = df[df['MMID'] != 'SIZE']
    df = df[df['MMID'] != 'BRUT']
    df = df[df['MMID'] != 'BTRD']
    df = df[df['MMID'] != 'MADF']
    df = df[df['MMID'] != 'TRIM']
    df = df[df['MMID'] != 'AUTO']
    df = df[df['BID'] != 0]
    df = df[df['OFR'] != 0]
    df = df[df['BID'] != df['OFR']]
    
    # Create a mid-price column (average of BID and OFR)
    print("mid")
    df['MID'] = (df['BID'] + df['OFR']) / 2
    print("dt")
    # Convert DATE and TIME into a single datetime column
    df['DATETIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'])
    print("sort")
    # Sort by date and time
    df = df.sort_values(by='DATETIME')
    
    # Set datetime as index for easy resampling
    df.set_index('DATETIME', inplace=True)
    print("return")
    # Calculate return for each 5-minute interval
    df_resampled = df['MID'].resample('5T').last()  # Get the last mid-price in each 5-minute interval
    df_resampled = df_resampled.ffill().pct_change()  # Forward fill missing data, then calculate percentage change
    
    # Calculate overnight return
    daily_mid = df['MID'].resample('D').last().ffill()  # Forward fill and get last price of each day
    overnight_return = daily_mid.pct_change()  # Calculate overnight return
    print("overnight")
    # Merge 5-minute and overnight returns into one DataFrame
    returns_df = pd.DataFrame({
        '5_min_return': df_resampled,
        'overnight_return': np.nan,
        'is_overnight': False  # Initialize all as False
    })
    
    # Assign overnight returns to the first available time point of each day
    for date, return_val in overnight_return.items():
        # Filter the DataFrame for the current date
        filtered_df = returns_df[returns_df.index.date == date]
        if not filtered_df.empty:
            # The first available time point in the 5-minute resampled data for the day
            first_time_point = filtered_df.index[0]
            returns_df.at[first_time_point, 'overnight_return'] = return_val
            returns_df.at[first_time_point, 'is_overnight'] = True  # Mark as True for overnight return
    
    # Drop rows where both returns are NaN
    returns_df.dropna(how='all', inplace=True)
    
    return returns_df

# Function to load CSV and apply calculations
def process_csv(filepath):
    # Load the CSV file
    df = pd.read_csv(filepath)
    
    # Ensure the BID and OFR columns are numeric
    df['BID'] = pd.to_numeric(df['BID'], errors='coerce')
    df['OFR'] = pd.to_numeric(df['OFR'], errors='coerce')
    
    # Drop rows with missing values
    df.dropna(subset=['BID', 'OFR'], inplace=True)
    
    # Calculate combined returns
    combined_returns = calculate_returns(df)
    
    return combined_returns



# Example usage
folder_path = r'C:\Users\Owner\Desktop\typ\jefferson\bollerslev-replication\minutereturnsspy\\'

x=1
# Iterate through all files in the folder
for filename in os.listdir(folder_path):
    print(x)
    file_path = os.path.join(folder_path, filename)
    combined_returns = process_csv(file_path)
    combined_returns.to_csv(rf"C:\Users\Owner\Desktop\typ\jefferson\bollerslev-replication\temporaryfiles\spyreturns{x}.csv")
    x =x+1
    


