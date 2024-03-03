import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os, time




def getPolygonDF(ticker , startdate , enddate , intervalperiod ):
    import json
    import time
    import pandas as pd
    import requests

    #My Polygon API (need to define in huggingface env variable)
    # mypolgonAPI = os.environ.get('mypolgonAPI') 
    
    #to get key from json file
    with open('config.json') as config_file:
        config = json.load(config_file)
    mypolgonAPI  = config['mypolgonAPI']

    headers = {"Authorization": f"Bearer {mypolgonAPI}"}

    interval = "minute"
    period = intervalperiod #"5"
    limit = 50000

    dflst = [] 
    nexturl = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{period}/{interval}/{startdate}/{enddate}?adjusted=true&sort=asc&limit={limit}"
    while True:
        r1 = requests.get(nexturl , headers=headers)
        print(r1)
        df = pd.DataFrame(json.loads(r1.text)["results"])
        df['UNIXTIME'] = pd.to_datetime(df['t'], unit='ms', utc=True).map(lambda x: x.tz_convert('America/New_York'))
        dflst.append(df)
        print(df.shape)
        time.sleep(15)
        try:
            nexturl = json.loads(r1.text)["next_url"] 
        except:
            break
    DF = pd.concat(dflst)
    return DF


##########################
DF = pd.read_csv('tickers.csv')

# Define the start and end dates for the last 2 years
end_date = datetime.now()
start_date = end_date - timedelta(days=2*365)

for ticker in list(DF.Ticker):
    print(ticker)

    current_date = start_date
    # Loop through each day in the last 2 years
    while current_date <= end_date:
        # Check if the current date is a weekday (Monday to Friday)
        if current_date.weekday() < 5:
            print(f"{current_date.strftime('%Y-%m-%d')} is a business day")
            current_datestr = current_date.strftime('%Y-%m-%d')
            if not os.path.exists(f'data\{current_datestr}'):
                os.makedirs(f'data\{current_datestr}')
            time.sleep(15)     #limitation free api
            df = getPolygonDF(ticker=ticker, startdate=current_datestr, enddate=current_datestr, intervalperiod='1')
            df.to_csv(f'data\{current_datestr}\{ticker}.csv')
        # Move to the next day
        current_date += timedelta(days=1)

#####
# test 1 year 5 api call/ minute

DF=pd.read_csv(r"\\192.168.1.1\New Volume\storage\premarket\america_2024-02-23.csv")
DF = DF.query("`Market Capitalization`<1e9")

end_date = datetime.now()
start_date = end_date - timedelta(days=1*365)

# for ticker in list(DF.Ticker):
#     print(ticker)
#     df = getPolygonDF(ticker=ticker, startdate=start_date.strftime('%Y-%m-%d'), enddate=end_date.strftime('%Y-%m-%d'), intervalperiod='1')
#     df.to_csv(f'./data/{ticker}.csv')

import time

# List of tickers
tickers = list(DF.Ticker)

# Number of API calls made
calls_made = 0

# Track the starting time
start_time = time.time()

# Main loop
for ticker in tickers:
    print(ticker)
    # Check if a minute has passed
    if time.time() - start_time >= 60:
        # Reset the starting time and the call count
        start_time = time.time()
        calls_made = 0

    # Check if the number of API calls made is less than 5
    if calls_made < 5:
        try:
        # Call the API function
            df = getPolygonDF(ticker=ticker, startdate=start_date.strftime('%Y-%m-%d'),
                          enddate=end_date.strftime('%Y-%m-%d'), intervalperiod='1')
        # Save the data to CSV
            df.to_csv(f'./data/{ticker}.csv')
        except:
            pass

        # Increment the count of API calls
        calls_made += 1
    else:
        # Wait for the next minute
        time_to_sleep = 60 - (time.time() - start_time)
        if time_to_sleep > 0:
            print(f"Exceeded call limit. Waiting for {time_to_sleep:.2f} seconds...")
            time.sleep(time_to_sleep)
