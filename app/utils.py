import pandas as pd
import requests
import datetime as dt

from database import engine, Base


def init_db():
    # session = SessionLocal()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    get_index_data()

def get_index_data():
    
    index_metadata = {
        'boors': 32097828799138957,
        'faraboors': 43685683301327984
    }
    
    temp = []
    for index_type, index_code in index_metadata.items():
        url = f'https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_code}'
        data = get_tmc_data(url)
        temp.append(transform_index_data(data, index_type, index_code))
    temp = pd.concat(temp)
    temp.to_sql(
        'IndexHistory',
        con= engine,
        if_exists= 'append',
        index= False
    )

def get_tmc_data(url):
    headers = { 
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
        }

    response = requests.get(url, headers= headers)
    result = response.json()

    return result

def transform_index_data(data, index_type, index_code):
    df = pd.DataFrame(data['indexB2'])

    df['Date_En'] = df['dEven'].astype(str).apply(lambda x: dt.datetime(int(x[:4]), int(x[4:6]), int(x[6:8])))
    df['Index_Id'] = df['insCode']
    df['Value'] = df['xNivInuPbMresIbs']
    df['Previous_Value'] = df['xNivInuClMresIbs']
    df['Index_Type'] = index_type
    df = df[['Index_Id', 'Index_Type', 'Date_En', 'Value', 'Previous_Value']]
    df['Closed_Day'] = False

    time_frame = pd.DataFrame({
        'Date_En': pd.date_range(start='2022-01-01', end=dt.datetime.now()),
        'Index_Id': index_code,
        'Index_Type': index_type
        })

    df = pd.merge(time_frame, df, how= 'left')
    df['Closed_Day'] = df['Closed_Day'].fillna(True)
    df[['Value', 'Previous_Value']] = df[['Value', 'Previous_Value']].fillna(method='ffill')

    return df
