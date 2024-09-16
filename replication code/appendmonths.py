import os
import pandas as pd
import numpy as np

# Define the folder path where CSV files are stored
folder_path = r'C:\Users\Owner\Desktop\typ\jefferson\bollerslev-replication\temporaryfiles'

# Define the time range for filtering (9:30 AM to 4:00 PM)
start_time = '09:30:00'
end_time = '17:00:00'

# Create an empty master dataframe to store all valid rows
master_df = pd.DataFrame()

# Iterate over each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        
        # Read the CSV file into a pandas dataframe
        df = pd.read_csv(file_path)
        df['5_min_return'] = pd.to_numeric(df['5_min_return'], errors='coerce')
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df[(df['5_min_return'] != 0) & (df['5_min_return'].notna())]

        # Convert the DATETIME column to a pandas datetime object
        df['DATETIME'] = pd.to_datetime(df['DATETIME'], format='%Y-%m-%d %H:%M:%S')
        
        # Filter rows where the time portion of the DATETIME is between 9:30 AM and 4 PM
        df_filtered = df[(df['DATETIME'].dt.time >= pd.to_datetime(start_time).time()) & 
                         (df['DATETIME'].dt.time <= pd.to_datetime(end_time).time())]
        
        # Calculate the monthly volatility of the 5_min_return column
        monthly_volatility = df_filtered.groupby(df_filtered['DATETIME'].dt.to_period('M'))['5_min_return'].var()
        
        # Convert the Series to a DataFrame and reset index
        monthly_volatility_df = monthly_volatility.reset_index()
        
        # Rename the columns
        monthly_volatility_df.columns = ['yearmonth', 'volatility']
        
        # Append to the master dataframe
        master_df = pd.concat([master_df, monthly_volatility_df], ignore_index=True)

# Convert 'yearmonth' to datetime format, representing the end of the month
master_df['yearmonth'] = master_df['yearmonth'].apply(lambda x: x.to_timestamp(freq='M'))

# Sort the dataframe by 'yearmonth' column
master_df_sorted = master_df.sort_values(by='yearmonth')

# Reset the index after sorting (optional, but useful for clean DataFrame)
master_df_sorted = master_df_sorted.reset_index(drop=True)

# Save the sorted dataframe to a CSV file
master_df_sorted.to_csv(r"C:\Users\Owner\Desktop\typ\jefferson\bollerslev-replication\temporaryfiles\monthlyvol\monthlyvol.csv", index=False)

print(master_df_sorted)
