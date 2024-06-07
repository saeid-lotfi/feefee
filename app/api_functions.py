from models import *

def get_overview(db):

    bourse_t_index_data = db.query(IndexState).filter(IndexState.Index_Type == 'total').order_by(IndexState.Date_Local.desc()).first()

    bourse_w_index_data = db.query(IndexState).filter(IndexState.Index_Type == 'total').order_by(IndexState.Date_Local.desc()).first()

    # gold_coin_data = db.query(GoldHistory).filter(GoldHistory.Gold_Type == 'coin-tamam').all()
    
    dollar_data = db.query(CryptocurrencyState).filter(CryptocurrencyState.Crypto_Id == 'teter').order_by(CryptocurrencyState.Date_Local.desc()).first()

    bitcoin_data = db.query(CryptocurrencyState).filter(CryptocurrencyState.Crypto_Id == 'bitcoin').order_by(CryptocurrencyState.Date_Local.desc()).first()
    
    # fund_n_risk_data = db.query(FundHistory).filter(FundHistory.Fund_Type == 'no-risk').all()

    return [
        bourse_t_index_data,
        bourse_w_index_data,
        # gold_coin_data,
        dollar_data,
        bitcoin_data,
        # fund_n_risk_data
    ]

def get_chart_data(db, candle, from_time, to_time):

    bourse_t_index_data = db.query(BourseHistory).filter(BourseHistory.Index_Type == 'total').all()

    bourse_w_index_data = db.query(BourseHistory).filter(BourseHistory.Index_Type == 'weighted').all()

    gold_coin_data = db.query(GoldHistory).filter(GoldHistory.Gold_Type == 'coin-tamam').all()
    
    dollar_data = db.query(CurrencyHistory).filter(CurrencyHistory.Currency_Type == 'dollar').all()

    bitcoin_data = db.query(CryptocurrencyHistory).filter(CryptocurrencyHistory.Crypto_Type == 'bitcoin').all()
    
    fund_n_risk_data = db.query(FundHistory).filter(FundHistory.Index_Type == 'weighted').all()

    return [
        bourse_t_index_data,
        bourse_w_index_data,
        gold_coin_data,
        dollar_data,
        bitcoin_data,
        fund_n_risk_data
    ]

def get_assets(db):

    fund_assets_data = db.query(FundState).all()

    return fund_assets_data
