import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt 
  
def fill_missing_rows(df, time_frequency='1min'):
    # Convert to datetime first if 'UNIXTIME' is not in datetime format
    df['UNIXTIME'] = pd.to_datetime(df['UNIXTIME'],utc=True)
    
    # Set 'UNIXTIME' as the index
    df.set_index('UNIXTIME', inplace=True)
    
    # Resample and forward-fill the missing values
    df = df.resample(time_frequency).ffill()
    
    # Reset the index
    df.reset_index(inplace=True)
    
    return df

def get_dataframe_for_day(df, date_str):
    # Check if 'UNIXTIME' is already in datetime format, if not, convert it
    if not pd.api.types.is_datetime64_any_dtype(df['UNIXTIME']):
        df['UNIXTIME'] = pd.to_datetime(df['UNIXTIME'],utc=True)
    
    # Parse the input date string to a datetime object
    input_date = pd.to_datetime(date_str).date()
    
    # Filter the dataframe for the date of interest
    day_df = df[df['UNIXTIME'].dt.date == input_date]
    return day_df


pathcsvfiles = 'data'
path_export_timeseries= 'export'

# Create the directory if it doesn't exist
os.makedirs(path_export_timeseries, exist_ok=True)


# Select numerical columns to scale
numerical_columns = ['v', 'vw', 'o', 'c', 'h', 'l']


DFtotal = pd.read_csv('DFtotal2023.csv',index_col=0)

for index,row in DFtotal.iterrows():
    print(row.Ticker)
    print(row.filename)
    filenamedate = row.filename
    ticker = row.Ticker
    try:
        df = pd.read_csv(f'./{pathcsvfiles}/{ticker}.csv' ,index_col=0 )
        filled_df = fill_missing_rows(df)
        result_df = get_dataframe_for_day(filled_df, filenamedate)

        # Initialize MinMaxScaler
        scaler = MinMaxScaler()
        scaled_df = result_df.copy()
        scaled_df[numerical_columns] = scaler.fit_transform(result_df[numerical_columns])
        scaled_df.to_csv(f'{path_export_timeseries}\{ticker}{filenamedate}.csv')
        print(scaled_df.shape)
    except:
        pass
    
        
    
    
    
