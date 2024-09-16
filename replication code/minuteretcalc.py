import pandas as pd
import numpy as np
import os

# Function to calculate returns
def calculate_returns(df):
    print("removing")
    exclude_list = ['WEDN', 'WEDO', 'SIZE', 'BRUT', 'BTRD', 'MADF', 'TRIM', 'AUTO', 'NAQS', 'EDGX', 'EDGA', 'FLOW','TMBR']
    """df = df[df['BID'] != 0]
    df = df[~df['MMID'].isin(exclude_list)]
    
    df = df[df['OFR'] > 2]
    df = df[df['BID'] > 2]
    df = df[df['OFR'] < 299]
    df = df[df['BID'] != df['OFR']]
    df = df[df['BID'] - df['OFR'] <= 10]"""
    
    # Create a boolean mask for all the conditions
    mask = (
    (df['BID'] != 0) &
    (~df['MMID'].isin(exclude_list)) &
    (df['OFR'] > 2) &
    (df['BID'] > 2) &
    (df['OFR'] < 249) &
    (df['BID'] != df['OFR']) &
    (df['BID'] - df['OFR'] <= 10)
    )
    df = df[mask]
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
folder_path = r'C:\Users\rra3\Desktop\Fall 24\bollerslev-replication\minutereturnsspy'

x=1
# Iterate through all files in the folder
for filename in os.listdir(folder_path):
    print(x)
    file_path = os.path.join(folder_path, filename)
    combined_returns = process_csv(file_path)
    combined_returns.to_csv(rf"C:\Users\rra3\Desktop\Fall 24\bollerslev-replication\minutereturnsspy.csv")
    x =x+1
    


