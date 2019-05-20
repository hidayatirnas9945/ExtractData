import pandas as pd
from sqlalchemy import create_engine

def GetConnectionFromFile():
    file = open('Connection', 'r')
    conn = {}
    for each in file.readlines():
        conn[each.split(':')[0]] = each.split(':')[1].replace('\n', '')
    return conn

def weekday_id(x):
    day_id={
        'Monday':'Senin',
        'Tuesday':'Selasa',
        'Wednesday':'Rabu',
        'Thursday':'Kamis',
        'Friday':'Jumat',
        'Saturday':'Sabtu',
        'Sunday':'Minggu'
    }
    return day_id[x]

def month_id(x):
    month_id={
        'April':'April',
        'August':'Agustus',
        'December':'Desember',
        'February':'Februari',
        'January':'Januari',
        'July':'Juli',
        'June':'Juni',
        'March':'Maret',
        'May':'Mei',
        'November':'November',
        'October':'Oktober',
        'September':'September'
    }
    return month_id[x]

def create_date_table(start, end, id_start):
    df = pd.DataFrame({"date": pd.date_range(start, end)})
    df["day"]=df["date"].dt.day
    df["month"]=df["date"].dt.month
    df["year"]=df["date"].dt.year
    df["quarter"]=df["date"].dt.quarter
    df["week"]=df["date"].dt.weekofyear
    df["weekday_en"]=df["date"].dt.weekday_name
    df["month_en"]=df["date"].dt.month_name()
    df["weekday_id"]=df["weekday_en"].apply(weekday_id)
    df["month_id"]=df["month_en"].apply(month_id)
    df["id"]=range(id_start, id_start+len(df))

    cols=[]
    cols.append(df.columns[-1])
    for each in df.columns[:-2]:
        cols.append(each)
    return df[cols]

if __name__=='__main__':
    conn=GetConnectionFromFile()
    engine=create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (conn['username'], conn['password'], conn['host'], conn['port'], conn['database']))
    date_start='1900-01-01'
    date_end='2100-12-31'
    id_start=1
    df=create_date_table(start=date_start, end=date_end, id_start=id_start)

    df.to_sql(name='date_dimension', con=engine, schema='test', if_exists='append', index=False)