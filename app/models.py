from sqlalchemy import String, Boolean, Integer, Column, Float, BigInteger, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_view
from sqlalchemy import select, func

Base = declarative_base()


######### tables
class BourseHistory(Base):
    __tablename__ = 'BourseHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Index_Id = Column(BigInteger, nullable= False)
    Index_Type = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

class CryptocurrencyHistory(Base):
    __tablename__ = 'CryptocurrencyHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Crypto_Id = Column(BigInteger, nullable= False)
    Crypto_Type = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

class CurrencyHistory(Base):
    __tablename__ = 'CurrencyHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Currency_Id = Column(BigInteger, nullable= False)
    Currency_Type = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

class GoldHistory(Base):
    __tablename__ = 'GoldHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Gold_Id = Column(BigInteger, nullable= False)
    Gold_Type = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Value = Column(Float, nullable= False)
    Previous_Value = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)

class FundHistory(Base):
    __tablename__ = 'FundHistory'

    id = Column(Integer, primary_key= True, nullable= False, autoincrement=True)
    Fund_Id = Column(BigInteger, nullable= False)
    Fund_Name = Column(String, nullable= False)
    Fund_NameDetail = Column(String, nullable= False)
    Date_En = Column(Date, nullable= False)
    Price_Closing = Column(Float, nullable= False)
    Price_Yesterday = Column(Float, nullable= False)
    Closed_Day = Column(Boolean, nullable= False)
    


######### views
stmt = select(
    BourseHistory.Index_Type,
    BourseHistory.Value,
    ((BourseHistory.Value - func.lag(BourseHistory.Value).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value).over(order_by= BourseHistory.Date_En)).label('daily_rate_of_change'),
    ((BourseHistory.Value - func.lag(BourseHistory.Value, 7).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value, 7).over(order_by= BourseHistory.Date_En)).label('weekly_rate_of_change'),
    ((BourseHistory.Value - func.lag(BourseHistory.Value, 30).over(order_by= BourseHistory.Date_En)) / func.lag(BourseHistory.Value, 30).over(order_by= BourseHistory.Date_En)).label('monthly_rate_of_change'),
    ).where(BourseHistory.Index_Type == "total").limit(1)

class Bourse_Total_Index(Base):
    __table__ = create_view('Bourse_Total_Index', stmt, Base.metadata)