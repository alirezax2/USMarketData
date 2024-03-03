import pandas as pd

pathcsvfiles = 'data'

ticker = 'HTOO'

df = pd.read_csv(f'./{pathcsvfiles}/{ticker}.csv' ,index_col=0 )


# Assuming 'df' is your dataframe and that it's already sorted by UNIXTIME
# And assuming that the date-times are in a sortable format, e.g., a Timestamp or a datetime object

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

def get_data_between(df, start_date_time, end_date_time):
    # Convert 'UNIXTIME' to datetime if needed and then filter
    df['UNIXTIME'] = pd.to_datetime(df['UNIXTIME'],utc=True)
    
    # Filter based on provided start and end date and times
    mask = (df['UNIXTIME'] >= pd.to_datetime(start_date_time)) & (df['UNIXTIME'] <= pd.to_datetime(end_date_time))
    return df.loc[mask]




def get_dataframe_for_day(df, date_str):
    # Check if 'UNIXTIME' is already in datetime format, if not, convert it
    if not pd.api.types.is_datetime64_any_dtype(df['UNIXTIME']):
        df['UNIXTIME'] = pd.to_datetime(df['UNIXTIME'],utc=True)
    
    # Parse the input date string to a datetime object
    input_date = pd.to_datetime(date_str).date()
    
    # Filter the dataframe for the date of interest
    day_df = df[df['UNIXTIME'].dt.date == input_date]
    return day_df

# Example usage:
ticker =  'HTOO'
df = pd.read_csv(f'./{pathcsvfiles}/{ticker}.csv' ,index_col=0 )
filled_df = fill_missing_rows(df)
result_df = get_dataframe_for_day(filled_df, '2024-02-16')

# Make sure before calling the function that 'df' with your dataframe name.

# Example usage:
filled_df = fill_missing_rows(df)

filtered_df = get_data_between(filled_df, '2024-02-23 18:35:00-05:00', '2024-02-23 18:39:00-05:00')




def ensure_start_end_times(df, start_time='04:00:00', end_time='20:00:00'):
    # Convert 'UNIXTIME' to datetime if it's not already
    df['UNIXTIME'] = pd.to_datetime(df['UNIXTIME'],utc=True)
    
    # Prepare a date-only column for grouping
    df['date'] = df['UNIXTIME'].dt.date
    
    # Function to apply to each group
    def ensure_times(group):
        # Assuming the group is sorted by datetime
        # Check and fill for start time 04:00:00
        day_start = pd.to_datetime(str(group['date'].iloc[0]) + ' ' + start_time)
        if group['UNIXTIME'].iloc[0] != day_start:
            start_row = group.iloc[0].copy()
            start_row['UNIXTIME'] = day_start
            group = pd.concat([pd.DataFrame([start_row]), group], ignore_index=True)
            
        # Check and fill for end time 20:00:00
        day_end = pd.to_datetime(str(group['date'].iloc[-1]) + ' ' + end_time)
        if group['UNIXTIME'].iloc[-1] != day_end:
            end_row = group.iloc[-1].copy()
            end_row['UNIXTIME'] = day_end
            group = pd.concat([group, pd.DataFrame([end_row])], ignore_index=True)
        
        return group
    
    # Apply the function to each group and recombine
    df_filled = df.groupby('date').apply(ensure_times).reset_index(drop=True)
    
    # Drop the temporary 'date' column
    df_filled = df_filled.drop(columns='date')
    
    # Sort our dataframe
    df_filled = df_filled.sort_values(by='UNIXTIME').reset_index(drop=True)
    
    return df_filled

# Example usage:
# df_filled = ensure_start_end_times(df)


