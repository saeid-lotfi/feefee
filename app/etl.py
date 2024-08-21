import datetime as dt
import requests
import json
import pytz
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
# pd.set_option('future.no_silent_downcasting', True)
pd.set_option('display.max_columns', None)

from database import get_db
from models import *


# set logger
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#################### api helpers
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


#################### raw api
def get_index_historic_data(index_type_code):
    
    url = f'https://cdn.tsetmc.com/api/Index/GetIndexB2History/{index_type_code}'
    raw_data = get_tmc_data_by_url(url)
    
    return raw_data

def get_index_overview_data():
    
    url = f'https://cdn.tsetmc.com/api/MarketData/GetMarketOverview/1'
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


#################### transform raw data
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

def transform_index_overview_data(raw_data, index_metadata):
    
    # change to dataframe
    df = pd.DataFrame(raw_data['indexB1'])

    # get interest data
    df['Index_Id'] = df['insCode']
    df['Value'] = df['xDrNivJIdx004']
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
    df['Date_Local'] = df['Datetime_Local'].dt.date
    df['Value'] = df['close']
    df['Previous_Value'] = df['open']

    # remove extra data
    df = df[['Crypto_Id', 'Datetime_UTC', 'Datetime_Local', 'Date_Local', 'Value', 'Previous_Value']]

    return df


#################### get db data
def get_bourse_index_state_data(index_type):
    
    db_gen = get_db()
    db = next(db_gen)

    result = db.query(BourseHistory.Value, BourseHistory.Date_En) \
    .filter(BourseHistory.Index_Type == index_type) \
        .order_by(BourseHistory.Date_En.desc()) \
            .first()
    
    if result != None:
        latest_value, latest_date = result
        latest_date_1 = latest_date - dt.timedelta(days= 1)
        latest_date_7 = latest_date - dt.timedelta(days= 7)
        latest_date_30 = latest_date - dt.timedelta(days= 30)

        latest_value_1 = db.query(BourseHistory.Value) \
            .filter(BourseHistory.Index_Type == index_type, BourseHistory.Date_En == latest_date_1) \
                .order_by(BourseHistory.Date_En.desc()) \
                    .first()[0]

        latest_value_7 = db.query(BourseHistory.Value) \
            .filter(BourseHistory.Index_Type == index_type, BourseHistory.Date_En == latest_date_7) \
                .order_by(BourseHistory.Date_En.desc()) \
                    .first()[0]

        latest_value_30 = db.query(BourseHistory.Value) \
            .filter(BourseHistory.Index_Type == index_type, BourseHistory.Date_En == latest_date_30) \
                .order_by(BourseHistory.Date_En.desc()) \
                    .first()[0]
        
        daily_rate_of_change = (latest_value - latest_value_1) / latest_value_1
        weekly_rate_of_change = (latest_value - latest_value_7) / latest_value_7
        monthly_rate_of_change = (latest_value - latest_value_30) / latest_value_30

        record = {
            'Index_Type': index_type,
            'Date_Local': latest_date,
            'Latest_Value': latest_value,
            'Daily_Rate_Of_Change': daily_rate_of_change,
            'Weekly_Rate_Of_Change': weekly_rate_of_change,
            'Monthly_Rate_Of_Change': monthly_rate_of_change
        }

        return record

def get_fund_asset_state_data(fund_id):
    
    db_gen = get_db()
    db = next(db_gen)

    result = db.query(
        FundHistory.Fund_Name,
        FundHistory.Fund_NameDetail,
        FundHistory.Fund_TypeNumber,
        FundHistory.Fund_TypeName,
        FundHistory.Price_Closing, 
        FundHistory.Date_En) \
    .filter(FundHistory.Fund_Id == fund_id) \
        .order_by(FundHistory.Date_En.desc()) \
            .first()
    
    if result != None:
        fund_name, fund_name_detail, fund_type_number, fund_type_name, latest_value, latest_date = result
        latest_date_1 = latest_date - dt.timedelta(days= 1)
        latest_date_7 = latest_date - dt.timedelta(days= 7)
        latest_date_30 = latest_date - dt.timedelta(days= 30)

        latest_value_1 = db.query(FundHistory.Price_Closing) \
            .filter(FundHistory.Fund_Id == fund_id, FundHistory.Date_En == latest_date_1) \
                .order_by(FundHistory.Date_En.desc()) \
                    .first()[0]

        latest_value_7 = db.query(FundHistory.Price_Closing) \
            .filter(FundHistory.Fund_Id == fund_id, FundHistory.Date_En == latest_date_7) \
                .order_by(FundHistory.Date_En.desc()) \
                    .first()[0]

        latest_value_30 = db.query(FundHistory.Price_Closing) \
            .filter(FundHistory.Fund_Id == fund_id, FundHistory.Date_En == latest_date_30) \
                .order_by(FundHistory.Date_En.desc()) \
                    .first()[0]
        
        daily_rate_of_change = (latest_value - latest_value_1) / latest_value_1
        weekly_rate_of_change = (latest_value - latest_value_7) / latest_value_7
        monthly_rate_of_change = (latest_value - latest_value_30) / latest_value_30

        record = {
            'Fund_Id': fund_id,
            'Date_Local': latest_date,
            'Fund_Name': fund_name,
            'Fund_NameDetail': fund_name_detail,
            'Fund_TypeNumber': fund_type_number,
            'Fund_TypeName': fund_type_name,
            'Latest_Value': latest_value,
            'Daily_Rate_Of_Change': daily_rate_of_change,
            'Weekly_Rate_Of_Change': weekly_rate_of_change,
            'Monthly_Rate_Of_Change': monthly_rate_of_change
        }
        return record

def get_cryptocurrency_state_data(crypto_name):
    
    
    db_gen = get_db()
    db = next(db_gen)

    result = db.query(CryptocurrencyHistory.Value, CryptocurrencyHistory.Date_Local) \
    .filter(CryptocurrencyHistory.Crypto_Id == crypto_name) \
        .order_by(CryptocurrencyHistory.Datetime_Local.desc()) \
            .first()
    
    if result != None:
        latest_value, latest_date = result
        latest_date_1 = latest_date - dt.timedelta(days= 1)
        latest_date_7 = latest_date - dt.timedelta(days= 7)
        latest_date_30 = latest_date - dt.timedelta(days= 30)

        latest_value_1 = db.query(CryptocurrencyHistory.Value) \
            .filter(CryptocurrencyHistory.Crypto_Id == crypto_name, CryptocurrencyHistory.Date_Local == latest_date_1) \
                .order_by(CryptocurrencyHistory.Datetime_Local.desc()) \
                    .first()[0]

        latest_value_7 = db.query(CryptocurrencyHistory.Value) \
            .filter(CryptocurrencyHistory.Crypto_Id == crypto_name, CryptocurrencyHistory.Date_Local == latest_date_7) \
                .order_by(CryptocurrencyHistory.Datetime_Local.desc()) \
                    .first()[0]

        latest_value_30 = db.query(CryptocurrencyHistory.Value) \
            .filter(CryptocurrencyHistory.Crypto_Id == crypto_name, CryptocurrencyHistory.Date_Local == latest_date_30) \
                .order_by(CryptocurrencyHistory.Datetime_Local.desc()) \
                    .first()[0]
        
        daily_rate_of_change = (latest_value - latest_value_1) / latest_value_1
        weekly_rate_of_change = (latest_value - latest_value_7) / latest_value_7
        monthly_rate_of_change = (latest_value - latest_value_30) / latest_value_30

        record = {
            'Crypto_Id': crypto_name,
            'Date_Local': latest_date,
            'Latest_Value': latest_value,
            'Daily_Rate_Of_Change': daily_rate_of_change,
            'Weekly_Rate_Of_Change': weekly_rate_of_change,
            'Monthly_Rate_Of_Change': monthly_rate_of_change
        }

        return record


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
        'Date_Local': stmt.excluded.Date_Local,
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

def insert_index_state_data(record):
    stmt = insert(IndexState).values(record)
    stmt = stmt.on_conflict_do_update(
    index_elements =['Index_Type', 'Date_Local'],
    set_={
        'Latest_Value': stmt.excluded.Latest_Value,
        'Daily_Rate_Of_Change': stmt.excluded.Daily_Rate_Of_Change,
        'Weekly_Rate_Of_Change': stmt.excluded.Weekly_Rate_Of_Change,
        'Monthly_Rate_Of_Change': stmt.excluded.Monthly_Rate_Of_Change
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

def insert_fund_state_data(record):
    stmt = insert(FundState).values(record)
    stmt = stmt.on_conflict_do_update(
    index_elements =['Fund_Id', 'Date_Local'],
    set_={
        'Fund_Name': stmt.excluded.Fund_Name,
        'Fund_NameDetail': stmt.excluded.Fund_NameDetail,
        'Fund_TypeNumber': stmt.excluded.Fund_TypeNumber,
        'Fund_TypeName': stmt.excluded.Fund_TypeName,
        'Latest_Value': stmt.excluded.Latest_Value,
        'Daily_Rate_Of_Change': stmt.excluded.Daily_Rate_Of_Change,
        'Weekly_Rate_Of_Change': stmt.excluded.Weekly_Rate_Of_Change,
        'Monthly_Rate_Of_Change': stmt.excluded.Monthly_Rate_Of_Change
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

def insert_cryptocurrency_state_data(record):
    stmt = insert(CryptocurrencyState).values(record)
    stmt = stmt.on_conflict_do_update(
    index_elements =['Crypto_Id', 'Date_Local'],
    set_={
        'Latest_Value': stmt.excluded.Latest_Value,
        'Daily_Rate_Of_Change': stmt.excluded.Daily_Rate_Of_Change,
        'Weekly_Rate_Of_Change': stmt.excluded.Weekly_Rate_Of_Change,
        'Monthly_Rate_Of_Change': stmt.excluded.Monthly_Rate_Of_Change
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

