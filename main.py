import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os, time


DF = pd.read_csv('tickers.csv')


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
        # time.sleep(15)
        try:
            nexturl = json.loads(r1.text)["next_url"] 
        except:
            break
    DF = pd.concat(dflst)
    return DF


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
