import datetime as dt
import time

import pytz
import threading

from database import get_db
from models import *
from etl import *

# set logger
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

LOG_PATH_INDEX_HISTORIC = "/logs/sync_bourse_index_historic.log"
LOG_PATH_FUND_HISTORIC = "/logs/sync_fund_assets_historic.log"
LOG_PATH_CRYPTOCURRENCY_HISTORIC = "/logs/sync_cryptocurrency_historic.log"
LOG_PATH_INDEX_STATE = "/logs/sync_bourse_index_state.log"
LOG_PATH_FUND_STATE = "/logs/sync_fund_assets_state.log"
LOG_PATH_CRYPTOCURRENCY_STATE = "/logs/sync_cryptocurrency_state.log"


#################### loggers
def get_logger(logger_name, log_path):
    logger = logging.getLogger(logger_name)
    logger.addHandler(logging.FileHandler(log_path))
    logger.propagate = False
    return  logger

#################### sync historic 
def sync_bourse_index_historic():
    logger = get_logger('Index_Historic', LOG_PATH_INDEX_HISTORIC)
    index_metadata = {
        'total': 32097828799138957,
        'weighted': 67130298613737946
    }

    for index_type, index_type_code in index_metadata.items():
        logger.info(f'Calling source api for index {index_type}')
        raw_data = get_index_historic_data(index_type_code)
        logger.info(f'Transforming raw data')
        df = transform_index_historic_data(raw_data, index_type, index_type_code)
        logger.info(f'Data sample: \n {df.head()}')
        logger.info(f'Data sample: \n {df.tail()}')
        logger.info(f'Inserting to db')
        insert_index_historic_data(df)

def sync_fund_assets_historic():
    logger = get_logger('Fund_Historic', LOG_PATH_FUND_HISTORIC)

    # get list of fund assetes
    logger.info(f'reading metadata for fund list')
    raw_fund_metadata = get_fund_list_data()
    logger.info(f'Transformin fund list raw data')
    fund_metadata = transform_fund_list_data(raw_fund_metadata)

    # for every fund in list
    for row in fund_metadata.itertuples():
        logger.info(f'Calling source api for fund {row.Fund_Name}')
        raw_data = get_fund_historic_data(row.Fund_Id)
        logger.info(f'Transforming raw data of fund {row.Fund_Name}')
        df = transform_fund_historic_data(raw_data, row.Fund_Name, row.Fund_NameDetail, row.Fund_TypeNumber, row.Fund_TypeName)
        logger.info(f'Data sample: \n {df.head()}')
        logger.info(f'Data sample: \n {df.tail()}')
        logger.info(f'Inserting to db for fund {row.Fund_Name}')
        insert_fund_historic_data(df)
        time.sleep(0.5)

def sync_cryptocurrency_historic():
    logger = get_logger('Cryptocurrency_Historic', LOG_PATH_CRYPTOCURRENCY_HISTORIC)
    crypto_metadata = {
        'teter': 'usdt-irt',
        'bitcoin': 'btc-irt'
    }

    for crypto_name, crypto_symbol in crypto_metadata.items():
        # get maxiumim existing data time
        db_gen = get_db()
        db = next(db_gen)
        
        logger.info(f"Getting db latest time")
        latest_date = db.query(CryptocurrencyHistory.Datetime_Local) \
        .filter(CryptocurrencyHistory.Crypto_Id == crypto_name) \
            .order_by(CryptocurrencyHistory.Datetime_Local.desc()) \
                .first()
        if latest_date != None:
            from_time = latest_date[0]
        else:
            from_time = dt.datetime(2024, 3, 1, tzinfo= pytz.timezone('Asia/Tehran'))
        from_timestamp = int(from_time.timestamp())
        to_time = dt.datetime.now()
        to_timestamp = int(to_time.timestamp())
        logger.info(f"Getting data from {str(from_time)} to {str(to_time)}")
        logger.info(f"UNIX timestamp from {str(from_timestamp)} to {str(to_timestamp)}")

        logger.info(f'Calling source api for crypto {crypto_name}')
        raw_data = get_cryptocurrency_historic_data(crypto_symbol, from_timestamp, to_timestamp)
        logger.info(f'Transforming raw data')
        df = transform_cryptocurrency_historic_data(raw_data, crypto_name)
        logger.info(f'Data sample: \n {df.head()}')
        logger.info(f'Data sample: \n {df.tail()}')
        logger.info(f'Inserting to db')
        insert_cryptocurrency_historic_data(df)


#################### update historic 
def update_bourse_index_historic():
    logger = get_logger('Index_Historic', LOG_PATH_INDEX_HISTORIC)
    index_metadata = {
        'total': 32097828799138957,
        'weighted': 67130298613737946
    }

    logger.info(f'Calling source api for index overview')
    raw_data = get_index_overview_data()
    logger.info(f'Transforming raw data')
    df = transform_index_overview_data(raw_data, index_metadata)
    logger.info(f'Data sample: \n {df.head()}')
    logger.info(f'Data sample: \n {df.tail()}')
    logger.info(f'Inserting to db')
    insert_index_overview_data(df)

def update_fund_assets_historic():
    logger = get_logger('Fund_Historic', LOG_PATH_FUND_HISTORIC)

    # get list of fund assetes
    logger.info(f'reading metadata for fund list')
    # raw_fund_metadata = get_fund_list_data()
    # logger.info(f'Transformin fund list raw data')
    # fund_metadata = transform_fund_list_data(raw_fund_metadata)

    # for every fund in list
    # for row in fund_metadata.itertuples():
    #     logger.info(f'Calling source api for fund {row.Fund_Name}')
    #     raw_data = get_fund_historic_data(row.Fund_Id)
    #     logger.info(f'Transforming raw data of fund {row.Fund_Name}')
    #     df = transform_fund_historic_data(raw_data, row.Fund_Name, row.Fund_NameDetail, row.Fund_TypeNumber, row.Fund_TypeName)
    #     logger.info(f'Data sample: \n {df.head()}')
    #     logger.info(f'Data sample: \n {df.tail()}')
    #     logger.info(f'Inserting to db for fund {row.Fund_Name}')
    #     insert_fund_historic_data(df)
    #     time.sleep(0.5)

def update_cryptocurrency_historic():
    logger = get_logger('Cryptocurrency_Historic', LOG_PATH_CRYPTOCURRENCY_HISTORIC)
    crypto_metadata = {
        'teter': 'usdt-irt',
        'bitcoin': 'btc-irt'
    }

    for crypto_name, crypto_symbol in crypto_metadata.items():
        # get maxiumim existing data time
        db_gen = get_db()
        db = next(db_gen)
        
        latest_date = db.query(CryptocurrencyHistory.Datetime_Local) \
        .filter(CryptocurrencyHistory.Crypto_Id == crypto_name) \
            .order_by(CryptocurrencyHistory.Datetime_Local.desc()) \
                .first()
        if latest_date != None:
            from_time = latest_date[0]
        else:
            from_time = dt.datetime(2024, 3, 1, tzinfo= pytz.timezone('Asia/Tehran'))
        from_timestamp = int(from_time.timestamp())
        to_time = dt.datetime.now()
        to_timestamp = int(to_time.timestamp())
        logger.info(f"Getting data from {str(from_time)} to {str(to_time)}")
        logger.info(f"UNIX timestamp from {str(from_timestamp)} to {str(to_timestamp)}")

        logger.info(f'Calling source api for crypto {crypto_name}')
        raw_data = get_cryptocurrency_historic_data(crypto_symbol, from_timestamp, to_timestamp)
        logger.info(f'Transforming raw data')
        df = transform_cryptocurrency_historic_data(raw_data, crypto_name)
        logger.info(f'Data sample: \n {df.head()}')
        logger.info(f'Data sample: \n {df.tail()}')
        logger.info(f'Inserting to db')
        insert_cryptocurrency_historic_data(df)


#################### update state
def update_bourse_index_state():
    logger = get_logger('Index_State', LOG_PATH_INDEX_STATE)
    index_metadata = {
        'total': 32097828799138957,
        'weighted': 67130298613737946
    }

    for index_type, index_code in index_metadata.items():
        logger.info(f'Syncing index state for {index_type}')
        record = get_bourse_index_state_data(index_type)
        if record != None:
            logger.info(f'State of {index_type}:')
            logger.info(str(record))
            logger.info(f'Inserting to db')
            insert_index_state_data(record)
        else:
            logger.info(f'Not found {index_type}:')

def update_fund_assets_state():
    logger = get_logger('Fund_State', LOG_PATH_FUND_STATE)
    # get list of fund assetes
    logger.info(f'reading metadata for fund list')
    raw_fund_metadata = get_fund_list_data()
    logger.info(f'Transformin fund list raw data')
    fund_metadata = transform_fund_list_data(raw_fund_metadata)

    # for every fund in list
    for row in fund_metadata.itertuples():
        logger.info(f'Syncing fund state for {row.Fund_Name}')
        record = get_fund_asset_state_data(row.Fund_Id)
        if record != None:
            logger.info(f'State of {row.Fund_Name}:')
            logger.info(str(record))
            logger.info(f'Inserting to db')
            insert_fund_state_data(record)
        else:
            logger.info(f'Not found {row.Fund_Name}')

def update_cryptocurrency_state():
    logger = get_logger('Cryptocurrency_State', LOG_PATH_CRYPTOCURRENCY_STATE)
    crypto_metadata = {
        'teter': 'usdt-irt',
        'bitcoin': 'btc-irt'
    }

    for crypto_name, crypto_symbol in crypto_metadata.items():
        logger.info(f'Syncing crypto state for {crypto_name}')
        record = get_cryptocurrency_state_data(crypto_name)
        if record != None:
            logger.info(f'State of {crypto_name}:')
            logger.info(str(record))
            logger.info(f'Inserting to db')
            insert_cryptocurrency_state_data(record)
        else:
            logger.info(f'Not found {crypto_name}')


#################### scheduler
def sync_task_scheduler(sync_task, sleep_time, logger_name, log_path):
    logger = get_logger(logger_name, log_path)
    logger.info(f"Starting {sync_task.__name__} ...")
    while True:
        try:
            sync_task()
            logger.info(f"{sync_task.__name__} executed successfully")
            logger.info(f"next {sync_task.__name__} will be in {sleep_time} seconds ...")
            logger.info("#" * 100)
            time.sleep(sleep_time)
        except:
            logger.error(f"{sync_task.__name__} failed!")
            logger.info(f"next {sync_task.__name__} will be in {sleep_time} seconds ...")
            logger.info("#" * 100)


#################### main sync
def schedule_sync_db():

    sync_metadata_init = {
        'index_historic_daily_sync': (sync_bourse_index_historic, 86400, 'Index_Historic', LOG_PATH_INDEX_HISTORIC),
        'fund_historic_daily_sync': (sync_fund_assets_historic, 86400, 'Fund_Historic', LOG_PATH_FUND_HISTORIC),
        'crypto_historic_quarterly_sync': (sync_cryptocurrency_historic, 900, 'Cryptocurrency_Historic', LOG_PATH_CRYPTOCURRENCY_HISTORIC),
    }

    sync_metadata_update = {
        'index_historic_minutely_sync': (update_bourse_index_historic, 120, 'Index_Historic', LOG_PATH_INDEX_HISTORIC),
        'fund_historic_minutely_sync': (update_fund_assets_historic, 120, 'Fund_Historic', LOG_PATH_FUND_HISTORIC),
        'crypto_historic_minutely_sync': (update_cryptocurrency_historic, 120, 'Cryptocurrency_Historic', LOG_PATH_CRYPTOCURRENCY_HISTORIC),
        'index_state_update': (update_bourse_index_state, 60, 'Index_State', LOG_PATH_INDEX_STATE),
        'fund_state_update': (update_fund_assets_state, 60, 'Fund_State', LOG_PATH_FUND_STATE),
        'crypto_state_update': (update_cryptocurrency_state, 30, 'Cryptocurrency_State', LOG_PATH_CRYPTOCURRENCY_STATE),
    }

    for task, task_metadata in sync_metadata_init.items():
        background_thread = threading.Thread(target= sync_task_scheduler, args= (task_metadata[0], task_metadata[1], task_metadata[2], task_metadata[3]))
        background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
        background_thread.start()   

    time.sleep(30) 

    for task, task_metadata in sync_metadata_update.items():
        background_thread = threading.Thread(target= sync_task_scheduler, args= (task_metadata[0], task_metadata[1], task_metadata[2], task_metadata[3]))
        background_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
        background_thread.start()   
    
    

