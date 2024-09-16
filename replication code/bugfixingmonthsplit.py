import pandas as pd
import os

# Function to split data by month and save each to a new CSV
def split_csv_by_month(input_csv):
    # Read the data
    df = pd.read_csv(input_csv, parse_dates=['DATE'])
    
    # Ensure the output directory exists

    
    # Create a 'yearmonth' column as a string without modifying the 'DATE' column
    df['yearmonth'] = df['DATE'].dt.strftime('%Y%m')

    # Group by 'yearmonth' and save each group as a new CSV file
    for yearmonth, group in df.groupby('yearmonth'):
       
       group.to_csv(rf'C:\Users\Owner\Desktop\typ\jefferson\bollerslev-replication\minutereturnsspy\months07\{yearmonth}.csv', index=False)

# Example usage
input_csv = r'C:\Users\Owner\Desktop\typ\jefferson\bollerslev-replication\minutereturnsspy\qvwkoqore2ufgf9o.csv'  # replace with your actual CSV file path
split_csv_by_month(input_csv)
