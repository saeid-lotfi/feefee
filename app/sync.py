import requests
import datetime as dt
import time
import threading
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)

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

def get_fund_meta_data():
    
    url = 'https://cdn.tsetmc.com/api/ClosingPrice/GetRelatedCompany/68'
    data = get_tmc_data_by_url(url)

    df = pd.DataFrame(data['relatedCompany'])
    df['Fund_Id'] = df['insCode']
    df['Fund_Name'] = df['instrument'].apply(lambda x: x['lVal18AFC'])
    df['Fund_NameDetail'] = df['instrument'].apply(lambda x: x['lVal30'])
    df = df[['Fund_Id', 'Fund_Name', 'Fund_NameDetail']]
    
    return df


#################### tsetmc api
def get_index_historic_data(index_type_code):
    
    url = f'https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_type_code}'
    raw_data = get_tmc_data_by_url(url)
    
    return raw_data

def get_fund_historic_data(fund_id):

    url = f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{fund_id}/0'
    raw_data = get_tmc_data_by_url(url)
    
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

def transform_fund_historic_data(raw_data, fund_name, fund_name_detail):

    # change to dataframe
    df = pd.DataFrame(raw_data['closingPriceDaily'])

    # get interest data
    df['Fund_Id'] = df['insCode']
    df['Fund_Name'] = fund_name
    df['Fund_NameDetail'] = fund_name_detail
    df['Date_En'] = df['dEven'].astype(str).apply(lambda x: dt.datetime(int(x[:4]), int(x[4:6]), int(x[6:8])))
    df['Price_Closing'] = df['pClosing']
    df['Price_Yesterday'] = df['priceYesterday']
    df['Closed_Day'] = False

    # remove extra data
    df = df[['Fund_Id', 'Date_En', 'Fund_Name', 'Fund_NameDetail', 'Price_Closing', 'Price_Yesterday', 'Closed_Day']]

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
    df['Fund_NameDetail'] = df['Fund_NameDetail'].fillna(fund_name)
    df[['Price_Closing', 'Price_Yesterday']] = df[['Price_Closing', 'Price_Yesterday']].ffill()

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
    

#################### main sync
def sync_bourse_index():

    bourse_index_logger.info("Starting fund sync task")
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

    fund_assets_logger.info("Starting fund sync task")
    fund_metadata = get_fund_meta_data()

    for row in fund_metadata.itertuples():
        fund_assets_logger.info(f'Calling source api for fund {row.Fund_Name}')
        raw_data = get_fund_historic_data(row.Fund_Id)
        fund_assets_logger.info(f'Transforming raw data')
        df = transform_fund_historic_data(raw_data, row.Fund_Name, row.Fund_NameDetail)
        fund_assets_logger.info(f'Data sample: \n {df.head()}')
        fund_assets_logger.info(f'Data sample: \n {df.tail()}')
        fund_assets_logger.info(f'Inserting to db')
        insert_fund_historic_data(df)
        time.sleep(1)


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
        fund_assets_logger.info("next sync_bourse_index will be in 1 hour ...")
        time.sleep(3600)

def schedule_sync_db():
    
    # bourse index
    background_thread = threading.Thread(target= schedule_sync_bourse_index)
    background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
    background_thread.start()

    # fund assets
    background_thread = threading.Thread(target= schedule_sync_fund_assets)
    background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
    background_thread.start()
    

