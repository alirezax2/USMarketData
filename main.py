import pandas as pd


pd.read_csv('tcikers.csv')


def getPolygonDF(ticker , startdate , enddate , intervalperiod , window, window2):
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
    DF['SMA'] = DF.c.rolling(window=window).mean()
    DF['SMA2'] = DF.c.rolling(window=window2).mean()
    return DF
