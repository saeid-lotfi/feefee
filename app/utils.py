import pandas as pd
import requests
import datetime as dt
import time

from database import engine
from models import Base
import logging
logging.getLogger().setLevel(logging.INFO)



def init_db():
    
    logging.info('Droping Existing Tables ...')
    Base.metadata.drop_all(engine)
    
    logging.info('Creating Tables ...')
    Base.metadata.create_all(engine)
    
    logging.info('Loading Index Data ...')
    get_index_historic_data()
    
    logging.info('Loading Fund Data ...')
    get_fund_historic_data()

def get_index_historic_data():
    
    index_metadata = {
        'bourse': 32097828799138957,
        'bourse_hamvazn': 67130298613737946
    }
    
    temp = []
    for index_type, index_code in index_metadata.items():
        url = f'https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_code}'
        data = get_tmc_data_by_url(url)
        temp.append(transform_index_data(data, index_type, index_code))
    temp = pd.concat(temp)
    temp.to_sql(
        'IndexHistory',
        con= engine,
        if_exists= 'append',
        index= False
    )

def get_fund_historic_data():
    df_metadata = get_fund_meta_data()
    
    temp = []
    for row in df_metadata.itertuples():
        logging.info(f'Getting Fund of: {row.Fund_Name}')
        url = f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{row.Fund_Id}/0'
        data = get_tmc_data_by_url(url)
        temp.append(transform_fund_data(data, row.Fund_Name, row.Fund_NameDetail))
        time.sleep(1)
    temp = pd.concat(temp)
    temp.to_sql(
        'FundHistory',
        con= engine,
        if_exists= 'append',
        index= False
    )

def get_fund_meta_data():
    
    url = 'https://cdn.tsetmc.com/api/ClosingPrice/GetRelatedCompany/68'
    data = get_tmc_data_by_url(url)

    df = pd.DataFrame(data['relatedCompany'])
    df['Fund_Id'] = df['insCode']
    df['Fund_Name'] = df['instrument'].apply(lambda x: x['lVal18AFC'])
    df['Fund_NameDetail'] = df['instrument'].apply(lambda x: x['lVal30'])
    df = df[['Fund_Id', 'Fund_Name', 'Fund_NameDetail']]
    
    return df

def get_tmc_data_by_url(url):
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
    df[['Value', 'Previous_Value']] = df[['Value', 'Previous_Value']].ffill()

    return df

def transform_fund_data(data, fund_name, fund_name_detail):
    df = pd.DataFrame(data['closingPriceDaily'])

    df['Date_En'] = df['dEven'].astype(str).apply(lambda x: dt.datetime(int(x[:4]), int(x[4:6]), int(x[6:8])))
    df['Fund_Id'] = df['insCode']
    df['Fund_Name'] = fund_name
    df['Fund_NameDetail'] = fund_name_detail
    df['Price_Closing'] = df['pClosing']
    df['Price_Yesterday'] = df['priceYesterday']
    df = df[['Fund_Id', 'Fund_Name', 'Fund_NameDetail', 'Date_En', 'Price_Closing', 'Price_Yesterday']]
    df['Closed_Day'] = False

    time_frame = pd.DataFrame({
        'Date_En': pd.date_range(start='2022-01-01', end=dt.datetime.now()),
        'Fund_Id': df['Fund_Id'].iloc[0]
        })

    df = pd.merge(time_frame, df, how= 'left')
    df['Fund_Name'] = df['Fund_Name'].fillna(fund_name)
    df['Fund_NameDetail'] = df['Fund_NameDetail'].fillna(fund_name)
    df['Closed_Day'] = df['Closed_Day'].fillna(True)
    df[['Price_Closing', 'Price_Yesterday']] = df[['Price_Closing', 'Price_Yesterday']].ffill()

    return df
