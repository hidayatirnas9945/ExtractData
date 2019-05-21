import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def GetConnectionFromFile():
    file = open('Connection', 'r')
    conn = {}
    for each in file.readlines():
        conn[each.split(':')[0]] = each.split(':')[1].replace('\n', '')
    return conn

def create_time_table(id_start=0):
    df=pd.DataFrame({'date':pd.date_range(start='1900-01-01 00:00:00', end='1900-01-01 23:59:59', freq='1000ms')})
    df['hour']=df['date'].dt.hour
    df['minute']=df['date'].dt.minute
    df['second']=df['date'].dt.second
    df['milisecond']=np.nan
    df["id"]=range(id_start, id_start+len(df))

    cols=[]
    cols.append(df.columns[-1])
    for each in df.columns[:-2]:
        cols.append(each)
    return df[cols]

if __name__=='__main__':
    # df.to_excel('test_time.xlsx', index=False)
    conn=GetConnectionFromFile()
    engine=create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (conn['username'], conn['password'], conn['host'], conn['port'], conn['database']))
    
    df=create_time_table()
    df.to_sql('time_dimension', con=engine, schema='test', if_exists='append', index=False)