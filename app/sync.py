import requests
import datetime as dt
import time
import json
import pytz
import threading
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
pd.set_option('display.max_columns', None)

from database import get_db
from models import *

# set logger
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
bourse_index_logger = logging.getLogger("BourseIndex")
bourse_index_logger.addHandler(logging.FileHandler("/logs/sync_bourse_index.log"))
bourse_index_logger.propagate = False
fund_assets_logger  = logging.getLogger("FundAssets")
fund_assets_logger.addHandler(logging.FileHandler("/logs/sync_fund_assets.log"))
fund_assets_logger.propagate = False
cryptocurrency_logger  = logging.getLogger("CryptoCuccency")
cryptocurrency_logger.addHandler(logging.FileHandler("/logs/sync_cryptocurrency.log"))
cryptocurrency_logger.propagate = False

####################
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

def get_exir_data_by_url(url):

    response = requests.get(url)
    result = response.json()

    return result

#################### tsetmc api
def get_index_historic_data(index_type_code):
    
    url = f'https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_type_code}'
    raw_data = get_tmc_data_by_url(url)
    
    return raw_data

def get_fund_list_data():

    fund_list_file = 'data/fund_metadata.json'
    with open(fund_list_file, 'r') as file:
        raw_data = json.load(file)
    
    return raw_data

def get_fund_historic_data(fund_id):

    url = f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{fund_id}/0'
    raw_data = get_tmc_data_by_url(url)
    
    return raw_data

def get_cryptocurrency_historic_data(crypto_symbol, from_timestamp, to_timestamp):
    
    url = f'https://api.exir.io/v2/chart?symbol={crypto_symbol}&resolution=15&from={from_timestamp}&to={to_timestamp}'
    raw_data = get_exir_data_by_url(url)
    
    return raw_data

#################### transform tsetmc data
def transform_index_historic_data(raw_data, index_type, index_code):
    
    # change to dataframe
    df = pd.DataFrame(raw_data['indexB2'])

    # get interest data
    df['Index_Id'] = df['insCode']
    df['Index_Type'] = index_type
    df['Date_En'] = df['dEven'].astype(str).apply(lambda x: dt.datetime(int(x[:4]), int(x[4:6]), int(x[6:8])))
    df['Value'] = df['xNivInuPbMresIbs']
    df['Previous_Value'] = df['xNivInuClMresIbs']
    df['Closed_Day'] = False # available days are not closed days
    
    # remove extra data
    df = df[['Index_Id', 'Date_En', 'Index_Type', 'Value', 'Previous_Value', 'Closed_Day']]

    # build time frame and merge it with dataframe
    time_frame = pd.DataFrame({
        'Date_En': pd.date_range(start= '2022-01-01', end= dt.datetime.now()),
        'Index_Id': index_code,
        'Index_Type': index_type
        })
    df = pd.merge(time_frame, df, how= 'left')

    # mark new days as closed days
    df['Closed_Day'] = df['Closed_Day'].fillna(True)
    # backfill null values
    df[['Value', 'Previous_Value']] = df[['Value', 'Previous_Value']].ffill()

    return df

def transform_fund_list_data(raw_data):
    
    # change to dataframe
    df = pd.DataFrame(raw_data)

    # get interest data
    df['Fund_Id'] = df['insCode']
    df['Fund_Name'] = df['lVal18AFC']
    df['Fund_NameDetail'] = df['name']
    df['Fund_TypeNumber'] = df['typeNum']
    df['Fund_TypeName'] = df['typeName']

    # remove extra data
    df = df[['Fund_Id', 'Fund_Name', 'Fund_NameDetail', 'Fund_TypeNumber', 'Fund_TypeName']]

    return df

def transform_fund_historic_data(raw_data, fund_name, fund_name_detail, fund_type_number, fund_type_name):

    # change to dataframe
    df = pd.DataFrame(raw_data['closingPriceDaily'])

    # get interest data
    df['Fund_Id'] = df['insCode']
    df['Fund_Name'] = fund_name
    df['Fund_NameDetail'] = fund_name_detail
    df['Fund_TypeNumber'] = fund_type_number
    df['Fund_TypeName'] = fund_type_name
    df['Date_En'] = df['dEven'].astype(str).apply(lambda x: dt.datetime(int(x[:4]), int(x[4:6]), int(x[6:8])))
    df['Price_Closing'] = df['pClosing']
    df['Price_Yesterday'] = df['priceYesterday']
    df['Closed_Day'] = False

    # remove extra data
    df = df[['Fund_Id', 'Date_En', 'Fund_Name', 'Fund_NameDetail', 'Fund_TypeNumber', 'Fund_TypeName', 'Price_Closing', 'Price_Yesterday', 'Closed_Day']]

    # build time frame and merge it with dataframe
    time_frame = pd.DataFrame({
        'Date_En': pd.date_range(start='2022-01-01', end=dt.datetime.now()),
        'Fund_Id': df['Fund_Id'].iloc[0]
        })
    df = pd.merge(time_frame, df, how= 'left')

    # mark new days as closed days
    df['Closed_Day'] = df['Closed_Day'].fillna(True)
    # backfill null values
    df['Fund_Name'] = df['Fund_Name'].fillna(fund_name)
    df['Fund_NameDetail'] = df['Fund_NameDetail'].fillna(fund_name_detail)
    df['Fund_TypeNumber'] = df['Fund_TypeNumber'].fillna(fund_type_number)
    df['Fund_TypeName'] = df['Fund_TypeName'].fillna(fund_type_name)
    df[['Price_Closing', 'Price_Yesterday']] = df[['Price_Closing', 'Price_Yesterday']].ffill()

    return df

def transform_cryptocurrency_historic_data(raw_data, crypto_id):
    
    # change to dataframe
    df = pd.DataFrame(raw_data)

    # get interest data
    df['Crypto_Id'] = crypto_id
    df['Datetime_UTC'] = pd.to_datetime(df['time'])
    df['Datetime_Local'] = df['Datetime_UTC'].dt.tz_convert(pytz.timezone('Asia/Tehran')).dt.tz_localize(None)
    df['Value'] = df['close']
    df['Previous_Value'] = df['open']

    # remove extra data
    df = df[['Crypto_Id', 'Datetime_UTC', 'Datetime_Local', 'Value', 'Previous_Value']]

    return df

#################### push to db
def insert_index_historic_data(df):
    records = df.to_dict(orient= 'records')
    stmt = insert(BourseHistory).values(records)
    stmt = stmt.on_conflict_do_update(
    index_elements =['Index_Id', 'Date_En'],
    set_={
        'Index_Type': stmt.excluded.Index_Type,
        'Value': stmt.excluded.Value,
        'Previous_Value': stmt.excluded.Previous_Value,
        'Closed_Day': stmt.excluded.Closed_Day
        }
    )
    
    db_gen = get_db()
    db = next(db_gen)
    try:
        db.execute(stmt)
        db.commit()
    except Exception as e:
        # Rollback the transaction in case of error
        db.rollback()
        raise e
    finally:
        db.close()

def insert_fund_historic_data(df):
    records = df.to_dict(orient= 'records')
    stmt = insert(FundHistory).values(records)
    stmt = stmt.on_conflict_do_update(
    index_elements =['Fund_Id', 'Date_En'],
    set_={
        'Fund_Name': stmt.excluded.Fund_Name,
        'Fund_NameDetail': stmt.excluded.Fund_NameDetail,
        'Fund_TypeNumber': stmt.excluded.Fund_TypeNumber,
        'Fund_TypeName': stmt.excluded.Fund_TypeName,
        'Price_Closing': stmt.excluded.Price_Closing,
        'Price_Yesterday': stmt.excluded.Price_Yesterday,
        'Closed_Day': stmt.excluded.Closed_Day
        }
    )
    
    db_gen = get_db()
    db = next(db_gen)
    try:
        db.execute(stmt)
        db.commit()
    except Exception as e:
        # Rollback the transaction in case of error
        db.rollback()
        raise e
    finally:
        db.close()
    
def insert_cryptocurrency_historic_data(df):
    records = df.to_dict(orient= 'records')
    stmt = insert(CryptocurrencyHistory).values(records)
    stmt = stmt.on_conflict_do_update(
    index_elements =['Crypto_Id', 'Datetime_Local'],
    set_={
        'Datetime_UTC': stmt.excluded.Datetime_UTC,
        'Value': stmt.excluded.Value,
        'Previous_Value': stmt.excluded.Previous_Value
        }
    )
    
    db_gen = get_db()
    db = next(db_gen)
    try:
        db.execute(stmt)
        db.commit()
    except Exception as e:
        # Rollback the transaction in case of error
        db.rollback()
        raise e
    finally:
        db.close()

#################### main sync
def sync_bourse_index():

    bourse_index_logger.info("Starting bourse index sync task")
    index_metadata = {
        'total': 32097828799138957,
        'weighted': 67130298613737946
    }

    for index_type, index_type_code in index_metadata.items():
        bourse_index_logger.info(f'Calling source api for index {index_type}')
        raw_data = get_index_historic_data(index_type_code)
        bourse_index_logger.info(f'Transforming raw data')
        df = transform_index_historic_data(raw_data, index_type, index_type_code)
        bourse_index_logger.info(f'Data sample: \n {df.head()}')
        bourse_index_logger.info(f'Data sample: \n {df.tail()}')
        bourse_index_logger.info(f'Inserting to db')
        insert_index_historic_data(df)

def sync_fund_assets():

    # get list of fund assetes
    fund_assets_logger.info("Starting fund sync task")
    fund_assets_logger.info(f'Calling source api for fund list')
    raw_fund_metadata = get_fund_list_data()
    fund_assets_logger.info(f'Transformin fund list raw data')
    fund_metadata = transform_fund_list_data(raw_fund_metadata)

    # for every fund in list
    for row in fund_metadata.itertuples():
        fund_assets_logger.info(f'Calling source api for fund {row.Fund_Name}')
        raw_data = get_fund_historic_data(row.Fund_Id)
        fund_assets_logger.info(f'Transforming raw data of fund {row.Fund_Name}')
        df = transform_fund_historic_data(raw_data, row.Fund_Name, row.Fund_NameDetail, row.Fund_TypeNumber, row.Fund_TypeName)
        fund_assets_logger.info(f'Data sample: \n {df.head()}')
        fund_assets_logger.info(f'Data sample: \n {df.tail()}')
        fund_assets_logger.info(f'Inserting to db for fund {row.Fund_Name}')
        insert_fund_historic_data(df)
        time.sleep(1)

def sync_cryptocurrency():

    cryptocurrency_logger.info("Starting cryptocurrency sync task")
    crypto_metadata = {
        'teter': 'usdt-irt',
        'bitcoin': 'btc-irt'
    }

    from_time = dt.datetime(2024, 3, 1, tzinfo= pytz.timezone('Asia/Tehran'))
    from_timestamp = int(from_time.timestamp())
    to_time = dt.datetime.now()
    to_timestamp = int(to_time.timestamp())
    cryptocurrency_logger.info(f"Getting data from {str(from_time)} to {str(to_time)}")
    cryptocurrency_logger.info(f"UNIX timestamp from {str(from_timestamp)} to {str(to_timestamp)}")

    for crypto_name, crypto_symbol in crypto_metadata.items():
        cryptocurrency_logger.info(f'Calling source api for crypto {crypto_name}')
        raw_data = get_cryptocurrency_historic_data(crypto_symbol, from_timestamp, to_timestamp)
        cryptocurrency_logger.info(f'Transforming raw data')
        df = transform_cryptocurrency_historic_data(raw_data, crypto_name)
        cryptocurrency_logger.info(f'Data sample: \n {df.head()}')
        cryptocurrency_logger.info(f'Data sample: \n {df.tail()}')
        cryptocurrency_logger.info(f'Inserting to db')
        insert_cryptocurrency_historic_data(df)

#################### schedule
def schedule_sync_bourse_index():
    while True:
        sync_bourse_index()
        bourse_index_logger.info("sync_bourse_index executed successfully")
        bourse_index_logger.info("next sync_bourse_index will be in 5 minutes ...")
        time.sleep(300)

def schedule_sync_fund_assets():
    while True:
        sync_fund_assets()
        fund_assets_logger.info("sync_fund_assets executed successfully")
        fund_assets_logger.info("next sync_fund_assets will be in 1 hour ...")
        time.sleep(3600)

def schedule_sync_cryptocurrency():
    while True:
        sync_cryptocurrency()
        fund_assets_logger.info("sync_cryptocurrency executed successfully")
        fund_assets_logger.info("next sync_cryptocurrency will be in 5 minutes ...")
        time.sleep(300)

def schedule_sync_db():
    
    # bourse index
    # background_thread = threading.Thread(target= schedule_sync_bourse_index)
    # background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
    # background_thread.start()

    # # fund assets
    # background_thread = threading.Thread(target= schedule_sync_fund_assets)
    # background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
    # background_thread.start()

    # cryptocurrency
    background_thread = threading.Thread(target= schedule_sync_cryptocurrency)
    background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
    background_thread.start()
    

