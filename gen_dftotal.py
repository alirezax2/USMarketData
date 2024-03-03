import pandas as pd
import numpy as np

import datetime
import os

#Main path of cvs file premarket data
mainpath = r"\\192.168.1.1\New Volume\storage\premarket"
os.listdir(mainpath)

def renamecoldf(df):
    items=[]
    for item in df.columns:
        items.append(item.replace('%','_perc').replace('(','').replace(')','').replace('-','_').replace(' ','').replace('*','').replace(',',''))    
    df.columns=items    
    return df

#Generate data set
start = datetime.datetime.strptime("2023-02-26", "%Y-%m-%d")
end = datetime.datetime.strptime("2023-12-31", "%Y-%m-%d")
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

mylst = ['america_' + item.strftime("%Y-%m-%d") + '.csv' for item in date_generated if os.path.exists(os.path.join(mainpath,'america_' + item.strftime("%Y-%m-%d") + '.csv'))]

# mylst = []
# for item in date_generated:
#     filename = 'america_' + item.strftime("%Y-%m-%d") + '.csv'
#     if os.path.exists(os.path.join(mainpath,filename)):
#         mylst.append(filename)
        
dflst=[]
for item in mylst[:-1]:
    # if mylst.index(item)<len(mylst)-1:
    print(f"Reading {os.path.join(mainpath,item)}"); DF = pd.read_csv(os.path.join(mainpath,item))
    DF = renamecoldf(DF)
    DF['filename'] = item.split('.csv')[0].split('america_')[1]
    DF['PremarketGapHighper'] = 100*(DF.Pre_marketHigh-DF.Pre_marketOpen)/DF.Pre_marketOpen 

    cols = ['Ticker' , 'PremarketGapHighper' , 'Pre_marketChange_perc' , 'Pre_marketVolume' , 'MarketCapitalization' , 'SharesFloat' , 'Pre_marketClose', 'Industry' , 'Sector' , 'filename']
    df = DF.query("(PremarketGapHighper>8 or Pre_marketChange_perc>8 ) and Pre_marketVolume>1e6"  )[cols]

    nextitem = mylst[mylst.index(item)+1]
    cols2 = ['Ticker' ,  'Pre_marketOpen' , 'Pre_marketGap_perc']
    print(f"Reading {os.path.join(mainpath,nextitem)}"); DFnext = pd.read_csv(os.path.join(mainpath,nextitem))
    DFnext = renamecoldf(DFnext)
    dfnext = DFnext[cols2]

    DFfinal = pd.merge(df,dfnext)
    dflst.append(DFfinal)
DFtotal = pd.concat(dflst)
DFtotal['diff'] = 100*(DFtotal.Pre_marketOpen - DFtotal.Pre_marketClose) / DFtotal.Pre_marketClose
DFtotal['Rotation'] =  DFtotal['SharesFloat'] /DFtotal['Pre_marketVolume']
DFtotal.to_csv('DFtotal2023.csv')



